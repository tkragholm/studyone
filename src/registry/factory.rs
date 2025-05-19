//! Factory functions for creating unified registry loaders
//!
//! This module provides factory functions to create registry loaders that use the unified schema system.

use super::RegisterLoader;
use crate::RecordBatch;
use crate::error::{ParquetReaderError, Result};
use crate::registry::direct_registry_loader::DirectRegistryLoader;
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

/// Create a registry loader from a registry name using direct deserialization
pub fn registry_from_name(name: &str) -> Result<Arc<dyn RegisterLoader>> {
    match name.to_lowercase().as_str() {
        "akm" => {
            // Use direct deserialization for AKM
            let registry = DirectRegistryLoader::new("AKM");
            Ok(Arc::new(registry))
        }
        "bef" => {
            // Use direct deserialization for BEF
            let registry = DirectRegistryLoader::new("BEF");
            Ok(Arc::new(registry))
        }
        "dod" => {
            // Use direct deserialization for DOD
            let registry = DirectRegistryLoader::new("DOD");
            Ok(Arc::new(registry))
        }
        "dodsaarsag" => {
            // Use direct deserialization for DODSAARSAG
            let registry = DirectRegistryLoader::new("DODSAARSAG");
            Ok(Arc::new(registry))
        }
        "ind" => {
            // Use direct deserialization for IND
            let registry = DirectRegistryLoader::new("IND");
            Ok(Arc::new(registry))
        }
        "mfr" => {
            // Use direct deserialization for MFR
            let registry = DirectRegistryLoader::new("MFR");
            Ok(Arc::new(registry))
        }
        "uddf" => {
            // Use direct deserialization for UDDF
            let registry = DirectRegistryLoader::new("UDDF");
            Ok(Arc::new(registry))
        }
        "vnds" => {
            // Use direct deserialization for VNDS
            let registry = DirectRegistryLoader::new("VNDS");
            Ok(Arc::new(registry))
        }
        "lpr_adm" => {
            // Use direct deserialization for LPR_ADM
            let registry = DirectRegistryLoader::new("LPR_ADM");
            Ok(Arc::new(registry))
        }
        "lpr_diag" => {
            // Use direct deserialization for LPR_DIAG
            let registry = DirectRegistryLoader::new("LPR_DIAG");
            Ok(Arc::new(registry))
        }
        "lpr_bes" => {
            // Use direct deserialization for LPR_BES
            let registry = DirectRegistryLoader::new("LPR_BES");
            Ok(Arc::new(registry))
        }
        "lpr3_kontakter" => {
            // Use direct deserialization for LPR3_KONTAKTER
            let registry = DirectRegistryLoader::new("LPR3_KONTAKTER");
            Ok(Arc::new(registry))
        }
        "lpr3_diagnoser" => {
            // Use direct deserialization for LPR3_DIAGNOSER
            let registry = DirectRegistryLoader::new("LPR3_DIAGNOSER");
            Ok(Arc::new(registry))
        }
        _ => Err(ParquetReaderError::MetadataError(format!("Unknown registry: {name}")).into()),
    }
}

/// Create a registry loader based on a path using direct deserialization
pub fn registry_from_path(path: &Path) -> Result<Arc<dyn RegisterLoader>> {
    // Try to infer registry from directory name
    if let Some(dir_name) = path.file_name().and_then(|f| f.to_str()) {
        let lower_name = dir_name.to_lowercase();

        // Check for registry name patterns in the path
        if lower_name.contains("akm") {
            let registry = DirectRegistryLoader::new("AKM");
            return Ok(Arc::new(registry));
        } else if lower_name.contains("bef") {
            let registry = DirectRegistryLoader::new("BEF");
            return Ok(Arc::new(registry));
        } else if lower_name.contains("dod") && !lower_name.contains("dodsaarsag") {
            let registry = DirectRegistryLoader::new("DOD");
            return Ok(Arc::new(registry));
        } else if lower_name.contains("dodsaarsag") {
            let registry = DirectRegistryLoader::new("DODSAARSAG");
            return Ok(Arc::new(registry));
        } else if lower_name.contains("ind") {
            let registry = DirectRegistryLoader::new("IND");
            return Ok(Arc::new(registry));
        } else if lower_name.contains("mfr") || lower_name.contains("foedselsregister") {
            let registry = DirectRegistryLoader::new("MFR");
            return Ok(Arc::new(registry));
        } else if lower_name.contains("uddf") || lower_name.contains("uddannelse") {
            let registry = DirectRegistryLoader::new("UDDF");
            return Ok(Arc::new(registry));
        } else if lower_name.contains("vnds") || lower_name.contains("migration") {
            let registry = DirectRegistryLoader::new("VNDS");
            return Ok(Arc::new(registry));
        } else if lower_name.contains("lpr_adm") {
            let registry = DirectRegistryLoader::new("LPR_ADM");
            return Ok(Arc::new(registry));
        } else if lower_name.contains("lpr_diag") {
            let registry = DirectRegistryLoader::new("LPR_DIAG");
            return Ok(Arc::new(registry));
        } else if lower_name.contains("lpr_bes") {
            let registry = DirectRegistryLoader::new("LPR_BES");
            return Ok(Arc::new(registry));
        } else if lower_name.contains("lpr3_kontakter") {
            let registry = DirectRegistryLoader::new("LPR3_KONTAKTER");
            return Ok(Arc::new(registry));
        } else if lower_name.contains("lpr3_diagnoser") {
            let registry = DirectRegistryLoader::new("LPR3_DIAGNOSER");
            return Ok(Arc::new(registry));
        }
    }

    // If we can't infer from the path, return an error
    Err(ParquetReaderError::MetadataError(format!(
        "Could not determine registry type from path: {}",
        path.display()
    ))
    .into())
}

/// Load data from multiple registries and combine them using direct deserialization
pub fn load_multiple_registries(
    base_paths: &[(&str, &Path)], // (registry_name, path)
    pnr_filter: Option<&HashSet<String>>,
) -> Result<Vec<RecordBatch>> {
    use rayon::prelude::*;

    // Use parallel execution to load all registries simultaneously
    let results: Vec<Result<Vec<RecordBatch>>> = base_paths
        .par_iter()
        .map(|(registry_name, path)| {
            let registry = registry_from_name(registry_name)?;
            registry.load(path, pnr_filter)
        })
        .collect();

    // Combine all batches
    let mut all_batches = Vec::new();
    for result in results {
        match result {
            Ok(batches) => all_batches.extend(batches),
            Err(e) => return Err(e),
        }
    }

    Ok(all_batches)
}

/// Load data from multiple registries asynchronously and combine them using direct deserialization
pub async fn load_multiple_registries_async(
    base_paths: &[(&str, &Path)], // (registry_name, path)
    pnr_filter: Option<&HashSet<String>>,
) -> Result<Vec<RecordBatch>> {
    use futures::future::join_all;
    use tokio::task::spawn;

    // Map of registry names to paths for error reporting
    let registry_paths: Vec<(String, String)> = base_paths
        .iter()
        .map(|(name, path)| ((*name).to_string(), path.to_string_lossy().to_string()))
        .collect();

    // Create futures for each registry load operation
    let futures = base_paths
        .iter()
        .enumerate()
        .map(|(idx, (registry_name, path))| {
            let registry_name = (*registry_name).to_string();
            let path = path.to_path_buf();
            let pnr_filter = pnr_filter.cloned();

            // Spawn each load operation as a separate task
            spawn(async move {
                let registry = registry_from_name(&registry_name)?;
                let pnr_filter_ref = pnr_filter.as_ref();
                let result = registry.load_async(&path, pnr_filter_ref).await?;
                Ok::<_, ParquetReaderError>((idx, result))
            })
        });

    // Wait for all futures to complete
    let results = join_all(futures).await;

    // Process results
    let mut indexed_results: Vec<(usize, Vec<RecordBatch>)> = Vec::new();
    for (i, task_result) in results.into_iter().enumerate() {
        match task_result {
            Ok(result) => match result {
                Ok((idx, batches)) => indexed_results.push((idx, batches)),
                Err(e) => {
                    let (name, path) = &registry_paths[i];
                    return Err(ParquetReaderError::IoError(format!(
                        "Failed to load registry '{name}' from path '{path}': {e}"
                    ))
                    .into());
                }
            },
            Err(e) => {
                let (name, path) = &registry_paths[i];
                return Err(ParquetReaderError::IoError(format!(
                    "Task error loading registry '{name}' from path '{path}': {e}"
                ))
                .into());
            }
        }
    }

    // Sort results by original index to maintain order
    indexed_results.sort_by_key(|(idx, _)| *idx);

    // Combine all batches
    let all_batches = indexed_results
        .into_iter()
        .flat_map(|(_, batches)| batches)
        .collect();

    Ok(all_batches)
}

/// Load registry data from a file with time period information and convert it to Individuals
///
/// This function loads data from a single registry file, extracts the time period information
/// from the filename, and converts the data to Individual objects with the time period set.
///
/// # Arguments
///
/// * `file_path` - Path to the parquet file to load
/// * `registry_name` - Name of the registry (AKM, BEF, etc.)
/// * `pnr_filter` - Optional set of PNRs to filter the data by
///
/// # Returns
///
/// A Result containing a vector of Individual objects with time period information
pub fn load_registry_with_time_period(
    file_path: &Path,
    registry_name: &str,
    pnr_filter: Option<&HashSet<String>>,
) -> Result<Vec<crate::models::core::individual::Individual>> {
    use crate::models::core::individual::Individual;
    use log::info;
    
    info!("Loading {} data from {}", registry_name, file_path.display());
    
    // Create registry loader and load the file
    let registry_loader = registry_from_name(registry_name)?;
    let batches = registry_loader.load(file_path, pnr_filter)?;
    
    if batches.is_empty() {
        info!("No data found in {}", file_path.display());
        return Ok(Vec::new());
    }
    
    // Convert batches to individuals with time period information
    let mut all_individuals = Vec::new();
    
    for batch in &batches {
        info!("Processing batch with {} rows", batch.num_rows());
        
        // Convert batch to individuals with time period information
        match Individual::from_batch_with_time_period(batch, file_path, registry_name) {
            Ok(individuals) => {
                info!("Converted batch to {} individuals", individuals.len());
                all_individuals.extend(individuals);
            }
            Err(e) => {
                info!("Failed to convert batch to individuals: {}", e);
            }
        }
    }
    
    info!("Loaded {} individuals from {}", all_individuals.len(), file_path.display());
    Ok(all_individuals)
}

/// Load data from all time periods for a registry
///
/// This function loads data from all available time periods for a registry,
/// converts it to Individual objects, and returns them grouped by time period.
///
/// # Arguments
///
/// * `registry_dir` - Path to the directory containing registry files
/// * `registry_name` - Name of the registry (AKM, BEF, etc.)
/// * `date_range` - Optional date range to filter by
/// * `pnr_filter` - Optional set of PNRs to filter the data by
///
/// # Returns
///
/// A Result containing a map of time periods to Individual objects
pub fn load_registry_time_periods(
    registry_dir: &Path,
    registry_name: &str,
    date_range: Option<(chrono::NaiveDate, chrono::NaiveDate)>,
    pnr_filter: Option<&HashSet<String>>,
) -> Result<std::collections::BTreeMap<crate::models::core::individual::temporal::TimePeriod, Vec<crate::models::core::individual::Individual>>> {
    use crate::models::core::individual::temporal::TimePeriod;
    use crate::utils::io::paths::temporal::get_registry_time_period_files;
    use log::info;
    use std::collections::BTreeMap;
    
    info!("Loading {} data from all time periods in {}", registry_name, registry_dir.display());
    
    // Get all files with time periods
    let mut period_files = get_registry_time_period_files(registry_dir)?;
    
    // Filter by date range if specified
    if let Some((start_date, end_date)) = date_range {
        period_files = crate::utils::io::paths::temporal::filter_files_by_date_range(
            &period_files,
            start_date,
            end_date,
        );
    }
    
    if period_files.is_empty() {
        info!("No time period files found for {}", registry_name);
        return Ok(BTreeMap::new());
    }
    
    // Load data for each time period
    let mut result: BTreeMap<TimePeriod, Vec<crate::models::core::individual::Individual>> = BTreeMap::new();
    
    for (period, file_path) in period_files {
        info!("Loading {} data for period {:?} from {}", registry_name, period, file_path.display());
        
        // Load individuals for this time period
        let individuals = load_registry_with_time_period(&file_path, registry_name, pnr_filter)?;
        
        if !individuals.is_empty() {
            info!("Loaded {} individuals for period {:?}", individuals.len(), period);
            result.insert(period, individuals);
        } else {
            info!("No individuals loaded for period {:?}", period);
        }
    }
    
    info!("Loaded data from {} time periods for {}", result.len(), registry_name);
    Ok(result)
}