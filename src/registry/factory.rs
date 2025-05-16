//! Factory functions for creating unified registry loaders
//!
//! This module provides factory functions to create registry loaders that use the unified schema system.

use super::RegisterLoader;
use crate::RecordBatch;
use crate::error::{ParquetReaderError, Result};
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

/// Create a registry loader from a registry name using the unified schema system
pub fn registry_from_name(name: &str) -> Result<Arc<dyn RegisterLoader>> {
    match name.to_lowercase().as_str() {
        "akm" => {
            // Use the macro-based AKM registry for the new implementation
            let registry = super::akm::create_deserializer();
            Ok(Arc::new(registry))
        }
        "bef" => {
            // Use the macro-based BEF registry for the new implementation
            let registry = super::bef::create_deserializer();
            Ok(Arc::new(registry))
        }
        "dod" => {
            // Use the macro-based DOD registry for the new implementation
            let registry = super::death::dod::create_deserializer();
            Ok(Arc::new(registry))
        }
        "dodsaarsag" => {
            // Use the macro-based DODSAARSAG registry for the new implementation
            let registry = super::death::dodsaarsag::create_deserializer();
            Ok(Arc::new(registry))
        }
        "ind" => {
            // Use the macro-based IND registry for the new implementation
            let registry = super::ind::create_deserializer();
            Ok(Arc::new(registry))
        }
        "mfr" => {
            // Use the macro-based MFR registry for the new implementation
            let registry = super::mfr::create_deserializer();
            Ok(Arc::new(registry))
        }
        "uddf" => {
            // Use the macro-based UDDF registry for the new implementation
            let registry = super::uddf::create_deserializer();
            Ok(Arc::new(registry))
        }
        "vnds" => {
            // Use the macro-based VNDS registry for the new implementation
            let registry = super::vnds::create_deserializer();
            Ok(Arc::new(registry))
        }
        "lpr_adm" => {
            // Use the macro-based LPR ADM registry
            let registry = super::lpr::create_adm_deserializer();
            Ok(Arc::new(registry))
        }
        "lpr_diag" => {
            // Use the macro-based LPR DIAG registry
            let registry = super::lpr::create_diag_deserializer();
            Ok(Arc::new(registry))
        }
        "lpr_bes" => {
            // Use the macro-based LPR BES registry
            let registry = super::lpr::create_bes_deserializer();
            Ok(Arc::new(registry))
        }
        "lpr3_kontakter" => {
            // Use the macro-based LPR3 KONTAKTER registry
            let registry = super::lpr::create_kontakter_deserializer();
            Ok(Arc::new(registry))
        }
        "lpr3_diagnoser" => {
            // Use the macro-based LPR3 DIAGNOSER registry
            let registry = super::lpr::create_diagnoser_deserializer();
            Ok(Arc::new(registry))
        }
        _ => Err(ParquetReaderError::MetadataError(format!("Unknown registry: {name}")).into()),
    }
}

/// Create a registry loader based on a path using the unified schema system
pub fn registry_from_path(path: &Path) -> Result<Arc<dyn RegisterLoader>> {
    // Try to infer registry from directory name
    if let Some(dir_name) = path.file_name().and_then(|f| f.to_str()) {
        let lower_name = dir_name.to_lowercase();

        // Check for registry name patterns in the path
        if lower_name.contains("akm") {
            let registry = super::akm::create_deserializer();
            return Ok(Arc::new(registry));
        } else if lower_name.contains("bef") {
            let registry = super::bef::create_deserializer();
            return Ok(Arc::new(registry));
        } else if lower_name.contains("dod") && !lower_name.contains("dodsaarsag") {
            let registry = super::death::dod::create_deserializer();
            return Ok(Arc::new(registry));
        } else if lower_name.contains("dodsaarsag") {
            let registry = super::death::dodsaarsag::create_deserializer();
            return Ok(Arc::new(registry));
        } else if lower_name.contains("ind") {
            let registry = super::ind::create_deserializer();
            return Ok(Arc::new(registry));
        } else if lower_name.contains("mfr") || lower_name.contains("foedselsregister") {
            let registry = super::mfr::create_deserializer();
            return Ok(Arc::new(registry));
        } else if lower_name.contains("uddf") || lower_name.contains("uddannelse") {
            let registry = super::uddf::create_deserializer();
            return Ok(Arc::new(registry));
        } else if lower_name.contains("vnds") || lower_name.contains("migration") {
            let registry = super::vnds::create_deserializer();
            return Ok(Arc::new(registry));
        } else if lower_name.contains("lpr_adm") {
            let registry = super::lpr::create_adm_deserializer();
            return Ok(Arc::new(registry));
        } else if lower_name.contains("lpr_diag") {
            let registry = super::lpr::create_diag_deserializer();
            return Ok(Arc::new(registry));
        } else if lower_name.contains("lpr_bes") {
            let registry = super::lpr::create_bes_deserializer();
            return Ok(Arc::new(registry));
        } else if lower_name.contains("lpr3_kontakter") {
            let registry = super::lpr::create_kontakter_deserializer();
            return Ok(Arc::new(registry));
        } else if lower_name.contains("lpr3_diagnoser") {
            let registry = super::lpr::create_diagnoser_deserializer();
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

/// Load data from multiple registries and combine them using the unified schema system
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

/// Load data from multiple registries asynchronously and combine them using the unified schema system
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