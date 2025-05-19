//! Temporal Registry Loader
//!
//! This module provides a registry loader that understands temporal data,
//! where each file represents data from a specific time period.

use crate::RecordBatch;
use crate::error::Result;
use crate::models::core::individual::temporal::TimePeriod;
use crate::registry::RegisterLoader;
use crate::utils::io::paths::temporal::get_registry_time_period_files;
use arrow::datatypes::SchemaRef;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use rayon::prelude::*;
use log::info;

/// Registry loader that understands time period data
///
/// This loader can handle files with time period information in their names
/// and provides methods for loading data from specific time periods.
pub struct TemporalRegistryLoader {
    /// The registry name
    registry_name: &'static str,
    /// The inner loader for actual deserialization
    inner_loader: Arc<dyn RegisterLoader>,
    /// PNR column name (if any)
    pnr_column: Option<&'static str>,
}

impl TemporalRegistryLoader {
    /// Create a new temporal registry loader
    ///
    /// # Arguments
    ///
    /// * `registry_name` - The name of the registry
    /// * `inner_loader` - The loader to use for actual deserialization
    ///
    /// # Returns
    ///
    /// A new TemporalRegistryLoader
    #[must_use]
    pub fn new(registry_name: &'static str, inner_loader: Arc<dyn RegisterLoader>) -> Self {
        // Get PNR column from inner loader
        let pnr_column = inner_loader.get_pnr_column_name();

        Self {
            registry_name,
            inner_loader,
            pnr_column,
        }
    }

    /// Load data from a specific time period
    ///
    /// # Arguments
    ///
    /// * `base_path` - Path to the registry directory
    /// * `time_period` - The time period to load data from
    /// * `pnr_filter` - Optional set of PNRs to filter the data by
    ///
    /// # Returns
    ///
    /// A Result containing a tuple with the time period and vector of record batches
    pub fn load_time_period(
        &self,
        base_path: &Path,
        time_period: TimePeriod,
        pnr_filter: Option<&HashSet<String>>,
    ) -> Result<(TimePeriod, Vec<RecordBatch>)> {
        // Find all files with time periods
        let period_files = get_registry_time_period_files(base_path)?;

        // Find the file for the requested time period
        if let Some(file_path) = period_files.get(&time_period) {
            info!(
                "Loading {} data for time period {:?} from {}",
                self.registry_name,
                time_period,
                file_path.display()
            );

            // Use the inner loader to load the file
            let batches = self.inner_loader.load(file_path, pnr_filter)?;
            Ok((time_period, batches))
        } else {
            info!(
                "No data found for {} in time period {:?}",
                self.registry_name, time_period
            );
            Ok((time_period, Vec::new()))
        }
    }

    /// Load data from multiple time periods
    ///
    /// # Arguments
    ///
    /// * `base_path` - Path to the registry directory
    /// * `time_periods` - The time periods to load data from
    /// * `pnr_filter` - Optional set of PNRs to filter the data by
    ///
    /// # Returns
    ///
    /// A Result containing a vector of tuples with time periods and record batches
    pub fn load_time_periods(
        &self,
        base_path: &Path,
        time_periods: &[TimePeriod],
        pnr_filter: Option<&HashSet<String>>,
    ) -> Result<Vec<(TimePeriod, Vec<RecordBatch>)>> {
        // Find all files with time periods
        let period_files = get_registry_time_period_files(base_path)?;

        // Load files for the requested time periods in parallel
        let pnr_filter_arc = pnr_filter.map(std::sync::Arc::new);
        
        let results: Vec<Result<(TimePeriod, Vec<RecordBatch>)>> = time_periods
            .par_iter()
            .map(|&period| {
                if let Some(file_path) = period_files.get(&period) {
                    info!(
                        "Loading {} data for time period {:?} from {}",
                        self.registry_name,
                        period,
                        file_path.display()
                    );

                    // Use the inner loader to load the file
                    let pnr_filter_ref = pnr_filter_arc.as_deref().map(|v| &**v);
                    match self.inner_loader.load(file_path, pnr_filter_ref) {
                        Ok(batches) => Ok((period, batches)),
                        Err(e) => Err(e),
                    }
                } else {
                    info!(
                        "No data found for {} in time period {:?}",
                        self.registry_name, period
                    );
                    Ok((period, Vec::new()))
                }
            })
            .collect();

        // Combine all the results, propagating any errors
        let mut combined_results = Vec::new();
        for result in results {
            combined_results.push(result?);
        }

        Ok(combined_results)
    }

    /// Get all available time periods for this registry
    ///
    /// # Arguments
    ///
    /// * `base_path` - Path to the registry directory
    ///
    /// # Returns
    ///
    /// A Result containing a vector of time periods
    pub fn get_available_time_periods(&self, base_path: &Path) -> Result<Vec<TimePeriod>> {
        // Find all files with time periods
        let period_files = get_registry_time_period_files(base_path)?;
        
        // Return the time periods as a sorted vector
        Ok(period_files.keys().copied().collect())
    }
}

impl RegisterLoader for TemporalRegistryLoader {
    fn get_register_name(&self) -> &'static str {
        self.registry_name
    }

    fn get_schema(&self) -> SchemaRef {
        self.inner_loader.get_schema()
    }

    fn get_pnr_column_name(&self) -> Option<&'static str> {
        self.pnr_column
    }

    fn get_join_column_name(&self) -> Option<&'static str> {
        self.inner_loader.get_join_column_name()
    }

    fn load_async<'a>(
        &'a self,
        base_path: &'a Path,
        pnr_filter: Option<&'a HashSet<String>>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<RecordBatch>>> + Send + 'a>> {
        // Create a future that loads all time periods
        let base_path = base_path.to_owned(); // Clone the base_path
        let inner_loader = self.inner_loader.clone(); // Clone the Arc
        
        Box::pin(async move {
            // Find all files with time periods
            let period_files = tokio::task::spawn_blocking(move || {
                get_registry_time_period_files(&base_path)
            })
            .await
            .map_err(|e| anyhow::anyhow!("Task join error: {}", e))??;

            // Load all files in parallel
            let file_paths: Vec<PathBuf> = period_files.values().cloned().collect();
            
            if file_paths.is_empty() {
                return Ok(Vec::new());
            }

            // Use the inner loader to load all files
            let mut all_batches = Vec::new();
            
            for path in file_paths {
                let batches = inner_loader.load(&path, pnr_filter)?;
                all_batches.extend(batches);
            }

            Ok(all_batches)
        })
    }
}