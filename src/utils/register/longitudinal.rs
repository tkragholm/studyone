//! Utilities for working with longitudinal registry data
//!
//! This module provides utilities for working with longitudinal registry data,
//! where data is provided in different files for different time periods.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use chrono::NaiveDate;
use log::info;

use crate::error::Result;
use crate::models::core::individual::temporal::TimePeriod;
use crate::models::core::individual::Individual;
use crate::registry::{RegisterLoader, temporal_registry_loader::TemporalRegistryLoader};
use crate::utils::io::paths::time_period;

/// Registry data grouped by time period
pub type TemporalRegistryData = BTreeMap<TimePeriod, Vec<Individual>>;

/// Configuration for the longitudinal data loading
pub struct LongitudinalConfig {
    /// Base directory for data
    pub data_dir: PathBuf,
    /// Map of registry names to directory names
    pub registry_dirs: HashMap<String, String>,
    /// Date range to include
    pub date_range: Option<(NaiveDate, NaiveDate)>,
    /// Optional PNR filter
    pub pnr_filter: Option<HashSet<String>>,
}

impl LongitudinalConfig {
    /// Create a new default config
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            registry_dirs: HashMap::new(),
            date_range: None,
            pnr_filter: None,
        }
    }

    /// Add a registry mapping
    pub fn add_registry(&mut self, name: impl Into<String>, dir_name: impl Into<String>) -> &mut Self {
        self.registry_dirs.insert(name.into(), dir_name.into());
        self
    }

    /// Set date range
    pub fn with_date_range(&mut self, start: NaiveDate, end: NaiveDate) -> &mut Self {
        self.date_range = Some((start, end));
        self
    }

    /// Set PNR filter
    pub fn with_pnr_filter(&mut self, pnrs: HashSet<String>) -> &mut Self {
        self.pnr_filter = Some(pnrs);
        self
    }

    /// Get the directory for a registry
    pub fn get_registry_dir(&self, registry: &str) -> Option<PathBuf> {
        self.registry_dirs.get(registry).map(|dir| self.data_dir.join(dir))
    }
}

/// Load registry data with time period information
///
/// This function loads data from registry files, extracting time period information
/// from filenames and associating it with the loaded individuals.
///
/// # Arguments
///
/// * `registry_loader` - The registry loader to use
/// * `config` - The longitudinal configuration
///
/// # Returns
///
/// A Result containing a map of time periods to individuals
pub fn load_longitudinal_data(
    registry_loader: std::sync::Arc<dyn RegisterLoader>,
    config: &LongitudinalConfig,
) -> Result<TemporalRegistryData> {
    let registry_name = registry_loader.get_register_name();
    
    // Get the registry directory
    let registry_dir = match config.get_registry_dir(registry_name) {
        Some(dir) => dir,
        None => {
            info!("No directory mapping found for registry {}", registry_name);
            return Ok(BTreeMap::new());
        }
    };

    // Create a temporal loader with the registry loader
    let temporal_loader = TemporalRegistryLoader::new(
        registry_name, 
        registry_loader.clone()
    );
    
    // Get available time periods
    let time_periods = temporal_loader.get_available_time_periods(&registry_dir)?;
    
    // Filter time periods by date range if specified
    let filtered_periods = if let Some((start, end)) = config.date_range {
        time_periods
            .into_iter()
            .filter(|period| {
                let period_start = period.start_date();
                let period_end = period.end_date();
                
                // Check for overlap
                period_end >= start && period_start <= end
            })
            .collect::<Vec<_>>()
    } else {
        time_periods
    };
    
    if filtered_periods.is_empty() {
        info!("No time periods found for registry {}", registry_name);
        return Ok(BTreeMap::new());
    }
    
    // Load data for each time period
    let pnr_filter_ref = config.pnr_filter.as_ref();
    let batches = temporal_loader.load_time_periods(&registry_dir, &filtered_periods, pnr_filter_ref)?;
    
    // Convert batches to individuals with time period information
    let mut result = BTreeMap::new();
    
    for (period, period_batches) in batches {
        if period_batches.is_empty() {
            continue;
        }
        
        let mut individuals = Vec::new();
        
        for batch in period_batches {
            info!(
                "Processing batch with {} rows for {} period {}",
                batch.num_rows(),
                registry_name,
                period.to_string()
            );
            
            if let Ok(mut batch_individuals) = Individual::from_batch(&batch) {
                info!(
                    "Converted batch to {} individuals for {} period {}",
                    batch_individuals.len(),
                    registry_name,
                    period.to_string()
                );
                
                // Update each individual with time period information
                for individual in &mut batch_individuals {
                    // Set the time period for this registry
                    individual.set_current_time_period(registry_name.to_string(), period);
                    
                    // Add the time period to the individual's time periods map
                    individual.add_time_period(
                        registry_name.to_string(),
                        period,
                        format!("{}_{}_{}", registry_name, period.to_string(), batch.num_rows()),
                    );
                }
                
                individuals.extend(batch_individuals);
            } else {
                info!(
                    "Failed to convert batch to individuals for {} period {}",
                    registry_name,
                    period.to_string()
                );
            }
        }
        
        if !individuals.is_empty() {
            info!(
                "Adding {} individuals for {} period {}",
                individuals.len(),
                registry_name,
                period.to_string()
            );
            result.insert(period, individuals);
        } else {
            info!(
                "No individuals processed for {} period {}",
                registry_name,
                period.to_string()
            );
        }
    }
    
    Ok(result)
}

/// Detect registry time periods from the files structure
///
/// # Arguments
///
/// * `data_dir` - Base directory for registry data
///
/// # Returns
///
/// A map of registry names to their available time periods
pub fn detect_registry_time_periods(data_dir: impl AsRef<Path>) -> Result<HashMap<String, Vec<TimePeriod>>> {
    let data_dir = data_dir.as_ref();
    let mut result = HashMap::new();
    
    // Read subdirectories (registry folders)
    if let Ok(entries) = std::fs::read_dir(data_dir) {
        for entry in entries {
            if let Ok(entry) = entry {
                let path = entry.path();
                
                if path.is_dir() {
                    if let Some(registry_name) = path.file_name().and_then(|n| n.to_str()) {
                        info!("Checking directory '{}' for time period files", registry_name);
                        
                        // Find all files with time periods in this registry folder
                        let period_files = time_period::find_time_period_files(&path);
                        
                        info!("Found {} time period files in directory '{}'", period_files.len(), registry_name);
                        
                        if !period_files.is_empty() {
                            // Extract time periods
                            let periods: Vec<TimePeriod> = period_files.into_iter()
                                .map(|(file_path, period)| {
                                    // Convert from time_period::TimePeriod to models::core::individual::temporal::TimePeriod
                                    let tp = match period {
                                        time_period::TimePeriod::Year(year) => 
                                            TimePeriod::Year(year),
                                        time_period::TimePeriod::Month(year, month) => 
                                            TimePeriod::Month(year, month),
                                        time_period::TimePeriod::Quarter(year, quarter) => 
                                            TimePeriod::Quarter(year, quarter),
                                    };
                                    
                                    info!("  File: {}, Period: {:?}", 
                                          file_path.file_name().unwrap_or_default().to_string_lossy(), 
                                          tp);
                                    
                                    tp
                                })
                                .collect();
                            
                            // Add to result with lowercase registry name for consistency
                            let registry_key = registry_name.to_lowercase();
                            info!("Adding registry '{}' with {} time periods", registry_key, periods.len());
                            result.insert(registry_key, periods);
                        } else {
                            info!("No time period files found in directory '{}'", registry_name);
                        }
                    }
                }
            }
        }
    }
    
    // Log the final detected registries
    info!("Detected {} registries with time period data:", result.len());
    for (registry, periods) in &result {
        info!("  Registry '{}': {} time periods", registry, periods.len());
    }
    
    Ok(result)
}

/// Merge individuals from multiple time periods for a registry
///
/// This function merges individuals from different time periods, using the latest data
/// for each individual when conflicts occur.
///
/// # Arguments
///
/// * `temporal_data` - Map of time periods to individuals
///
/// # Returns
///
/// A vector of merged individuals
pub fn merge_temporal_individuals(temporal_data: &TemporalRegistryData) -> Vec<Individual> {
    let mut individuals_by_pnr: HashMap<String, Individual> = HashMap::new();
    
    // Process time periods in chronological order (earliest to latest)
    for (_, individuals) in temporal_data {
        for individual in individuals {
            match individuals_by_pnr.get_mut(&individual.pnr) {
                // Update existing individual with newer data
                Some(existing) => {
                    existing.merge_fields(individual);
                },
                // Add new individual
                None => {
                    individuals_by_pnr.insert(individual.pnr.clone(), individual.clone());
                },
            }
        }
    }
    
    individuals_by_pnr.into_values().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_detect_registry_time_periods() {
        // This test can't be run automatically since it depends on the actual file system
        // In a real implementation, we would use a mock file system
    }
    
    #[test]
    fn test_merge_temporal_individuals() {
        // Create test data
        let mut data = BTreeMap::new();
        
        // Create individuals for two time periods
        let period1 = TimePeriod::Year(2020);
        let period2 = TimePeriod::Year(2021);
        
        // Individual 1 appears in both periods
        let mut individual1_period1 = Individual::new("1234".to_string(), None);
        individual1_period1.set_current_time_period("TEST".to_string(), period1);
        individual1_period1.gender = Some("M".to_string());
        
        let mut individual1_period2 = Individual::new("1234".to_string(), None);
        individual1_period2.set_current_time_period("TEST".to_string(), period2);
        individual1_period2.gender = Some("M".to_string());
        individual1_period2.marital_status = Some("G".to_string()); // Newer data
        
        // Individual 2 only appears in period 2
        let mut individual2_period2 = Individual::new("5678".to_string(), None);
        individual2_period2.set_current_time_period("TEST".to_string(), period2);
        individual2_period2.gender = Some("F".to_string());
        
        // Add to the data map
        data.insert(period1, vec![individual1_period1]);
        data.insert(period2, vec![individual1_period2, individual2_period2]);
        
        // Merge the data
        let merged = merge_temporal_individuals(&data);
        
        // We should have 2 individuals
        assert_eq!(merged.len(), 2);
        
        // Find individual 1 and verify it has the newer marital status
        let merged_individual1 = merged.iter().find(|i| i.pnr == "1234").unwrap();
        assert_eq!(merged_individual1.gender, Some("M".to_string()));
        assert_eq!(merged_individual1.marital_status, Some("G".to_string()));
        
        // Find individual 2
        let merged_individual2 = merged.iter().find(|i| i.pnr == "5678").unwrap();
        assert_eq!(merged_individual2.gender, Some("F".to_string()));
    }
}