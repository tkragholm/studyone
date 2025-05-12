//! Factory functions for creating registry loaders
//!
//! This module provides factory functions to create registry loaders from names or paths.

use super::RegisterLoader;
use crate::DodRegister;
use crate::DodsaarsagRegister;
use crate::RecordBatch;
use crate::error::{ParquetReaderError, Result};
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

/// Create a registry loader from a registry name
pub fn registry_from_name(name: &str) -> Result<Arc<dyn RegisterLoader>> {
    match name.to_lowercase().as_str() {
        "akm" => Ok(Arc::new(super::akm::AkmRegister::new())),
        "bef" => Ok(Arc::new(super::bef::BefRegister::new())),
        "dod" => Ok(Arc::new(DodRegister::new())),
        "dodsaarsag" => Ok(Arc::new(DodsaarsagRegister::new())),
        "ind" => Ok(Arc::new(super::ind::IndRegister::new())),
        "mfr" => Ok(Arc::new(super::mfr::MfrRegister::new())),
        "uddf" => Ok(Arc::new(super::uddf::UddfRegister::new())),
        "vnds" => Ok(Arc::new(super::vnds::VndsRegister::new())),
        "lpr_adm" => Ok(Arc::new(super::lpr::LprAdmRegister::new())),
        "lpr_diag" => Ok(Arc::new(super::lpr::LprDiagRegister::new())),
        "lpr_bes" => Ok(Arc::new(super::lpr::LprBesRegister::new())),
        "lpr3_kontakter" => Ok(Arc::new(super::lpr::Lpr3KontakterRegister::new())),
        "lpr3_diagnoser" => Ok(Arc::new(super::lpr::Lpr3DiagnoserRegister::new())),
        _ => Err(ParquetReaderError::MetadataError(format!("Unknown registry: {name}")).into()),
    }
}

/// Create a registry loader based on a path (inferring the registry type from the path)
pub fn registry_from_path(path: &Path) -> Result<Arc<dyn RegisterLoader>> {
    // Try to infer registry from directory name
    if let Some(dir_name) = path.file_name().and_then(|f| f.to_str()) {
        let lower_name = dir_name.to_lowercase();

        // Check for registry name patterns in the path
        if lower_name.contains("akm") {
            return Ok(Arc::new(super::akm::AkmRegister::new()));
        } else if lower_name.contains("bef") {
            return Ok(Arc::new(super::bef::BefRegister::new()));
        } else if lower_name.contains("dod") && !lower_name.contains("dodsaarsag") {
            return Ok(Arc::new(DodRegister::new()));
        } else if lower_name.contains("dodsaarsag") {
            return Ok(Arc::new(DodsaarsagRegister::new()));
        } else if lower_name.contains("ind") {
            return Ok(Arc::new(super::ind::IndRegister::new()));
        } else if lower_name.contains("mfr") || lower_name.contains("foedselsregister") {
            return Ok(Arc::new(super::mfr::MfrRegister::new()));
        } else if lower_name.contains("uddf") || lower_name.contains("uddannelse") {
            return Ok(Arc::new(super::uddf::UddfRegister::new()));
        } else if lower_name.contains("vnds") || lower_name.contains("migration") {
            return Ok(Arc::new(super::vnds::VndsRegister::new()));
        } else if lower_name.contains("lpr_adm") {
            return Ok(Arc::new(super::lpr::LprAdmRegister::new()));
        } else if lower_name.contains("lpr_diag") {
            return Ok(Arc::new(super::lpr::LprDiagRegister::new()));
        } else if lower_name.contains("lpr_bes") {
            return Ok(Arc::new(super::lpr::LprBesRegister::new()));
        } else if lower_name.contains("lpr3_kontakter") {
            return Ok(Arc::new(super::lpr::Lpr3KontakterRegister::new()));
        } else if lower_name.contains("lpr3_diagnoser") {
            return Ok(Arc::new(super::lpr::Lpr3DiagnoserRegister::new()));
        }
    }

    // If we can't infer from the path, return an error
    Err(ParquetReaderError::MetadataError(format!(
        "Could not determine registry type from path: {}",
        path.display()
    ))
    .into())
}

/// Load data from multiple registries and combine them
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

/// Load data from multiple registries asynchronously and combine them
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
