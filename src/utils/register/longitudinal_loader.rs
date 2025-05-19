//! Longitudinal data loader for comprehensive registry data
//!
//! This module provides utilities for loading data from multiple registries
//! and merging it into a comprehensive dataset with time period information.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::Mutex;
use log::{info, warn, debug};
use chrono::NaiveDate;

use crate::error::Result;
use crate::models::core::individual::temporal::TimePeriod;
use crate::models::core::individual::Individual;
use crate::models::core::traits::TemporalValidity;
use crate::registry::factory;
use crate::utils::register::longitudinal::{
    LongitudinalConfig, load_longitudinal_data, merge_temporal_individuals, detect_registry_time_periods,
};

/// Result of loading longitudinal data from multiple registries
pub struct LongitudinalDataset {
    /// All individuals in the dataset, merged from different time periods and registries
    pub individuals: Vec<Individual>,
    /// Map of registry name to time periods available
    pub registry_periods: Mutex<HashMap<String, Vec<TimePeriod>>>,
    /// Map of registry name to individuals by time period
    pub registry_data: Mutex<HashMap<String, BTreeMap<TimePeriod, Vec<Individual>>>>,
}

impl LongitudinalDataset {
    /// Create a new empty dataset
    #[must_use]
    pub fn new() -> Self {
        Self {
            individuals: Vec::new(),
            registry_periods: Mutex::new(HashMap::new()),
            registry_data: Mutex::new(HashMap::new()),
        }
    }

    /// Get the total number of individuals
    #[must_use]
    pub fn individual_count(&self) -> usize {
        self.individuals.len()
    }

    /// Get all time periods across all registries
    #[must_use]
    pub fn all_time_periods(&self) -> Vec<(String, TimePeriod)> {
        let mut periods = Vec::new();
        
        for (registry, time_periods) in &*self.registry_periods.lock().unwrap() {
            for &period in time_periods {
                periods.push((registry.clone(), period));
            }
        }
        
        periods
    }

    /// Get individuals valid at a specific date
    #[must_use]
    pub fn individuals_at_date(&self, date: &NaiveDate) -> Vec<&Individual> {
        self.individuals
            .iter()
            .filter(|ind| ind.was_valid_at(date))
            .collect()
    }

    /// Get the date range covered by this dataset
    #[must_use]
    pub fn date_range(&self) -> Option<(NaiveDate, NaiveDate)> {
        // Find the earliest and latest dates
        let mut earliest_date = None;
        let mut latest_date = None;
        
        for (_, periods) in &*self.registry_periods.lock().unwrap() {
            for period in periods {
                let period_start = period.start_date();
                let period_end = period.end_date();
                
                earliest_date = match earliest_date {
                    Some(date) if period_start < date => Some(period_start),
                    None => Some(period_start),
                    _ => earliest_date,
                };
                
                latest_date = match latest_date {
                    Some(date) if period_end > date => Some(period_end),
                    None => Some(period_end),
                    _ => latest_date,
                };
            }
        }
        
        if let (Some(start), Some(end)) = (earliest_date, latest_date) {
            Some((start, end))
        } else {
            None
        }
    }
}

impl Default for LongitudinalDataset {
    fn default() -> Self {
        Self::new()
    }
}

/// Load all longitudinal data from registries
///
/// This function loads data from all available registries and merges it into
/// a comprehensive dataset with time period information.
///
/// # Arguments
///
/// * `data_dir` - Base directory for registry data
/// * `date_range` - Optional date range to filter by
/// * `pnr_filter` - Optional set of PNRs to filter by
///
/// # Returns
///
/// A Result containing the longitudinal dataset
pub fn load_all_longitudinal_data(
    data_dir: impl AsRef<Path>,
    date_range: Option<(NaiveDate, NaiveDate)>,
    pnr_filter: Option<HashSet<String>>,
) -> Result<LongitudinalDataset> {
    let data_dir = data_dir.as_ref();
    info!("Loading all longitudinal data from {}", data_dir.display());
    
    // 1. Detect all registries and time periods
    let registry_periods = detect_registry_time_periods(data_dir)?;
    info!("Detected {} registries with time period data", registry_periods.len());

    // 2. Configure the longitudinal data loading
    let mut config = LongitudinalConfig::new(data_dir);
    
    // Add registry mappings - use lowercase for both for consistency
    for registry in registry_periods.keys() {
        let registry_lower = registry.to_lowercase();
        config.add_registry(&registry_lower, &registry_lower);
    }
    
    // Set date range if provided
    if let Some((start, end)) = date_range {
        config.with_date_range(start, end);
        info!("Filtering data by date range: {} to {}", start, end);
    }
    
    // Set PNR filter if provided
    if let Some(pnrs) = pnr_filter {
        config.with_pnr_filter(pnrs);
        info!("Filtering data by {} PNRs", config.pnr_filter.as_ref().unwrap().len());
    }
    
    // 3. Load data from each registry
    let mut dataset = LongitudinalDataset::new();
    {
        let mut periods = dataset.registry_periods.lock().unwrap();
        *periods = registry_periods.clone();
    }
    
    let mut all_individuals: HashMap<String, Individual> = HashMap::new();
    
    for registry_name in registry_periods.keys() {
        debug!("Loading data from registry '{}'", registry_name);
        
        // Create registry loader
        let registry_loader = match factory::registry_from_name(registry_name) {
            Ok(loader) => loader,
            Err(e) => {
                warn!("Failed to create loader for registry '{}': {}", registry_name, e);
                continue;
            }
        };
        
        // Load longitudinal data for this registry
        let temporal_data = match load_longitudinal_data(registry_loader, &config) {
            Ok(data) => data,
            Err(e) => {
                warn!("Failed to load data for registry '{}': {}", registry_name, e);
                continue;
            }
        };
        
        if temporal_data.is_empty() {
            debug!("No data loaded for registry '{}'", registry_name);
            continue;
        }
        
        // Store the registry data
        {
            let mut registry_data = dataset.registry_data.lock().unwrap();
            registry_data.insert(registry_name.clone(), temporal_data.clone());
        }
        
        // Merge individuals from different time periods for this registry
        let registry_individuals = merge_temporal_individuals(&temporal_data);
        debug!(
            "Merged {} time periods into {} individuals for '{}'",
            temporal_data.len(),
            registry_individuals.len(),
            registry_name
        );
        
        // Add to the overall individuals map
        for individual in registry_individuals {
            match all_individuals.get_mut(&individual.pnr) {
                Some(existing) => {
                    // Update existing individual with data from this registry
                    existing.merge_fields(&individual);
                }
                None => {
                    // Add new individual
                    all_individuals.insert(individual.pnr.clone(), individual);
                }
            }
        }
    }
    
    // Convert the map to a vector
    dataset.individuals = all_individuals.into_values().collect();
    info!("Loaded {} individuals from all registries", dataset.individuals.len());
    
    Ok(dataset)
}

/// Load registry data for specific registries
///
/// This function loads data from a specific list of registries and merges it into
/// a comprehensive dataset with time period information.
///
/// # Arguments
///
/// * `data_dir` - Base directory for registry data
/// * `registries` - List of registry names to load
/// * `date_range` - Optional date range to filter by
/// * `pnr_filter` - Optional set of PNRs to filter by
///
/// # Returns
///
/// A Result containing the longitudinal dataset
pub fn load_selected_longitudinal_data(
    data_dir: impl AsRef<Path>,
    registries: &[&str],
    date_range: Option<(NaiveDate, NaiveDate)>,
    pnr_filter: Option<HashSet<String>>,
) -> Result<LongitudinalDataset> {
    let data_dir = data_dir.as_ref();
    info!("Loading selected longitudinal data from {}", data_dir.display());
    
    // 1. Detect all registries and time periods
    let all_registry_periods = detect_registry_time_periods(data_dir)?;
    
    // Filter to only include the requested registries
    let registry_periods: HashMap<String, Vec<TimePeriod>> = all_registry_periods
        .into_iter()
        .filter(|(registry, _)| registries.contains(&registry.as_str()))
        .collect();
    
    info!("Selected {} registries for loading", registry_periods.len());
    
    // 2. Configure the longitudinal data loading
    let mut config = LongitudinalConfig::new(data_dir);
    
    // Add registry mappings - use lowercase for both for consistency
    for registry in registry_periods.keys() {
        let registry_lower = registry.to_lowercase();
        config.add_registry(&registry_lower, &registry_lower);
    }
    
    // Set date range if provided
    if let Some((start, end)) = date_range {
        config.with_date_range(start, end);
        info!("Filtering data by date range: {} to {}", start, end);
    }
    
    // Set PNR filter if provided
    if let Some(pnrs) = pnr_filter {
        config.with_pnr_filter(pnrs);
        info!("Filtering data by {} PNRs", config.pnr_filter.as_ref().unwrap().len());
    }
    
    // 3. Load data from each registry
    let mut dataset = LongitudinalDataset::new();
    {
        let mut periods = dataset.registry_periods.lock().unwrap();
        *periods = registry_periods.clone();
    }
    
    let mut all_individuals: HashMap<String, Individual> = HashMap::new();
    
    for registry_name in registry_periods.keys() {
        debug!("Loading data from registry '{}'", registry_name);
        
        // Create registry loader
        let registry_loader = match factory::registry_from_name(registry_name) {
            Ok(loader) => loader,
            Err(e) => {
                warn!("Failed to create loader for registry '{}': {}", registry_name, e);
                continue;
            }
        };
        
        // Load longitudinal data for this registry
        let temporal_data = match load_longitudinal_data(registry_loader, &config) {
            Ok(data) => data,
            Err(e) => {
                warn!("Failed to load data for registry '{}': {}", registry_name, e);
                continue;
            }
        };
        
        if temporal_data.is_empty() {
            debug!("No data loaded for registry '{}'", registry_name);
            continue;
        }
        
        // Store the registry data
        {
            let mut registry_data = dataset.registry_data.lock().unwrap();
            registry_data.insert(registry_name.clone(), temporal_data.clone());
        }
        
        // Merge individuals from different time periods for this registry
        let registry_individuals = merge_temporal_individuals(&temporal_data);
        debug!(
            "Merged {} time periods into {} individuals for '{}'",
            temporal_data.len(),
            registry_individuals.len(),
            registry_name
        );
        
        // Add to the overall individuals map
        for individual in registry_individuals {
            match all_individuals.get_mut(&individual.pnr) {
                Some(existing) => {
                    // Update existing individual with data from this registry
                    existing.merge_fields(&individual);
                }
                None => {
                    // Add new individual
                    all_individuals.insert(individual.pnr.clone(), individual);
                }
            }
        }
    }
    
    // Convert the map to a vector
    dataset.individuals = all_individuals.into_values().collect();
    info!("Loaded {} individuals from selected registries", dataset.individuals.len());
    
    Ok(dataset)
}