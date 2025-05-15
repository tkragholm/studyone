//! Factory functions for creating unified registry loaders
//!
//! This module provides factory functions to create registry loaders that use the unified schema system.

use super::RegisterLoader;
use crate::DodRegister;
use crate::DodsaarsagRegister;
use crate::RecordBatch;
use crate::error::{ParquetReaderError, Result};
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

/// Create a registry loader from a registry name using the unified schema system
pub fn registry_from_name(name: &str) -> Result<Arc<dyn RegisterLoader>> {
    match name.to_lowercase().as_str() {
        "akm" => todo!("Need to migrate to the new macro approach..."),
        "bef" => {
            // Use the macro-based BEF registry for the new implementation
            let registry = super::bef::create_deserializer();
            Ok(Arc::new(registry))
        }
        "dod" => Ok(Arc::new(DodRegister::new())), // TODO: Add unified system support
        "dodsaarsag" => Ok(Arc::new(DodsaarsagRegister::new())), // TODO: Add unified system support
        "ind" => {
            let mut register = super::ind::IndRegister::new();
            // Enable unified system for IND
            register.use_unified_system(true);
            Ok(Arc::new(register))
        }
        "mfr" => {
            let mut register = super::mfr::MfrRegister::new();
            // Enable unified system for MFR
            register.use_unified_system(true);
            Ok(Arc::new(register))
        }
        "uddf" => {
            let mut register = super::uddf::UddfRegister::new();
            // Enable unified system for UDDF
            register.use_unified_system(true);
            Ok(Arc::new(register))
        }
        "vnds" => {
            let mut register = super::vnds::VndsRegister::new();
            // Enable unified system for VNDS
            register.use_unified_system(true);
            Ok(Arc::new(register))
        }
        "lpr_adm" => {
            let mut register = super::lpr::LprAdmRegister::new();
            // Enable unified system for LPR_ADM
            register.use_unified_system(true);
            Ok(Arc::new(register))
        }
        "lpr_diag" => {
            let mut register = super::lpr::LprDiagRegister::new();
            // Enable unified system for LPR_DIAG
            register.use_unified_system(true);
            Ok(Arc::new(register))
        }
        "lpr_bes" => {
            let mut register = super::lpr::LprBesRegister::new();
            // Enable unified system for LPR_BES
            register.use_unified_system(true);
            Ok(Arc::new(register))
        }
        "lpr3_kontakter" => {
            let mut register = super::lpr::Lpr3KontakterRegister::new();
            // Enable unified system for LPR3_KONTAKTER
            register.use_unified_system(true);
            Ok(Arc::new(register))
        }
        "lpr3_diagnoser" => {
            let mut register = super::lpr::Lpr3DiagnoserRegister::new();
            // Enable unified system for LPR3_DIAGNOSER
            register.use_unified_system(true);
            Ok(Arc::new(register))
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
            todo!("Need to migrate to the new macro approach...")
        } else if lower_name.contains("bef") {
            let registry = super::bef::create_deserializer();
            return Ok(Arc::new(registry));
        } else if lower_name.contains("dod") && !lower_name.contains("dodsaarsag") {
            return Ok(Arc::new(DodRegister::new())); // TODO: Add unified system support
        } else if lower_name.contains("dodsaarsag") {
            return Ok(Arc::new(DodsaarsagRegister::new())); // TODO: Add unified system support
        } else if lower_name.contains("ind") {
            let mut register = super::ind::IndRegister::new();
            register.use_unified_system(true);
            return Ok(Arc::new(register));
        } else if lower_name.contains("mfr") || lower_name.contains("foedselsregister") {
            let mut register = super::mfr::MfrRegister::new();
            register.use_unified_system(true);
            return Ok(Arc::new(register));
        } else if lower_name.contains("uddf") || lower_name.contains("uddannelse") {
            let mut register = super::uddf::UddfRegister::new();
            register.use_unified_system(true);
            return Ok(Arc::new(register));
        } else if lower_name.contains("vnds") || lower_name.contains("migration") {
            let mut register = super::vnds::VndsRegister::new();
            register.use_unified_system(true);
            return Ok(Arc::new(register));
        } else if lower_name.contains("lpr_adm") {
            let mut register = super::lpr::LprAdmRegister::new();
            register.use_unified_system(true);
            return Ok(Arc::new(register));
        } else if lower_name.contains("lpr_diag") {
            let mut register = super::lpr::LprDiagRegister::new();
            register.use_unified_system(true);
            return Ok(Arc::new(register));
        } else if lower_name.contains("lpr_bes") {
            let mut register = super::lpr::LprBesRegister::new();
            register.use_unified_system(true);
            return Ok(Arc::new(register));
        } else if lower_name.contains("lpr3_kontakter") {
            let mut register = super::lpr::Lpr3KontakterRegister::new();
            register.use_unified_system(true);
            return Ok(Arc::new(register));
        } else if lower_name.contains("lpr3_diagnoser") {
            let mut register = super::lpr::Lpr3DiagnoserRegister::new();
            register.use_unified_system(true);
            return Ok(Arc::new(register));
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
