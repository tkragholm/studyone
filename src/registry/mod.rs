//! Registry definitions and loaders for various Danish data sources
//!
//! This module contains registry definitions and loaders for various Danish registry data sources.
//! It provides a unified interface for loading and processing parquet files from different
//! registry sources, with optimized loading strategies for each.
//!
//! This module also contains the implementations for converting registry data to domain models,
//! supporting direct model conversion capabilities.
//!
//! Available registers:
//! - AKM (Arbejdsklassifikationsmodulet): Employment information
//! - BEF (Befolkning): Population demographic information
//! - DOD (Deaths): Death records
//! - DODSAARSAG (Causes of death): Death cause information
//! - IND (Indkomst): Income and tax information
//! - LPR (Landspatientregistret): National Patient Registry (versions 2 and 3)
//! - MFR (Medical Birth Registry): Birth information
//! - UDDF (Uddannelse): Educational information
//! - VNDS (Vandringer/Migration): Migration information

use crate::RecordBatch;
use crate::Result;

use arrow::datatypes::SchemaRef;
use std::collections::HashSet;
use std::path::Path;

use std::future::Future;
use std::pin::Pin;

/// Base trait for registry loaders
pub trait RegisterLoader: Send + Sync {
    /// Get the name of the register
    fn get_register_name(&self) -> &'static str;

    /// Get the schema for this register
    fn get_schema(&self) -> SchemaRef;

    /// Load records from the register
    fn load(
        &self,
        base_path: &Path,
        pnr_filter: Option<&HashSet<String>>,
    ) -> Result<Vec<RecordBatch>> {
        // Check if we're already in a tokio runtime
        let current = tokio::runtime::Handle::try_current();

        if let Ok(_) = current {
            // We're already in a tokio runtime, use futures executor
            futures::executor::block_on(self.load_async(base_path, pnr_filter))
        } else {
            // Create a blocking runtime to run the async code
            let rt = tokio::runtime::Runtime::new()?;

            // Use the async implementation
            rt.block_on(self.load_async(base_path, pnr_filter))
        }
    }

    /// Load records from the register asynchronously
    ///
    /// This is the main implementation that handles both directory and file loading
    /// in an efficient and consistent way
    fn load_async<'a>(
        &'a self,
        base_path: &'a Path,
        pnr_filter: Option<&'a HashSet<String>>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<RecordBatch>>> + Send + 'a>> {
        // Get the schema for this registry
        let schema = self.get_schema();

        // Get the PNR column name if available
        let pnr_column = self.get_pnr_column_name();

        // Move into async block
        Box::pin(async move {
            // Check if path exists and is a directory or file
            let metadata = tokio::fs::metadata(base_path).await.map_err(|e| {
                anyhow::anyhow!("Failed to access path {}: {}", base_path.display(), e)
            })?;

            if metadata.is_dir() {
                // DIRECTORY HANDLING
                // Use a single, consistent approach to find parquet files in the directory
                log::info!("Loading from directory: {}", base_path.display());

                // Use tokio's async file operations for the initial file search
                let base_path_owned = base_path.to_path_buf(); // Create owned copy for the task
                let parquet_files = tokio::task::spawn_blocking(move || {
                    // Use the synchronous file finding utility which has parallel optimization
                    crate::utils::find_parquet_files(&base_path_owned)
                })
                .await
                .map_err(|e| anyhow::anyhow!("Task join error: {}", e))??;

                if parquet_files.is_empty() {
                    log::warn!(
                        "No parquet files found in directory: {}",
                        base_path.display()
                    );
                    return Ok(Vec::new());
                }

                log::info!(
                    "Found {} parquet files in {}",
                    parquet_files.len(),
                    base_path.display()
                );

                // Handle PNR filtering if needed
                if let Some(pnr_filter) = pnr_filter {
                    // If PNR column is available, apply filtering
                    if let Some(pnr_column) = pnr_column {
                        // Use tokio's spawn_blocking to leverage rayon's parallel processing
                        // This moves CPU-intensive work off the async runtime
                        let schema_ref = schema.clone();
                        let pnr_filter = pnr_filter.clone();

                        let base_path_owned = base_path.to_path_buf(); // Create owned copy
                        tokio::task::spawn_blocking(move || {
                            // Use the optimized parallel loading that handles PNR filtering
                            crate::utils::load_parquet_files_parallel(
                                &base_path_owned,
                                Some(schema_ref.as_ref()),
                                Some(&pnr_filter),
                                None,
                                None,
                            )
                        })
                        .await
                        .map_err(|e| anyhow::anyhow!("Task join error: {}", e))?
                    } else {
                        // PNR filtering requested but no PNR column available
                        log::warn!(
                            "PNR filtering requested but registry {} doesn't support PNR filtering",
                            self.get_register_name()
                        );

                        // Fall back to loading without filtering
                        let schema_ref = schema.clone();
                        let base_path_owned = base_path.to_path_buf(); // Create owned copy
                        tokio::task::spawn_blocking(move || {
                            crate::utils::load_parquet_files_parallel(
                                &base_path_owned,
                                Some(schema_ref.as_ref()),
                                None::<&HashSet<String>>, // No filtering
                                None,
                                None,
                            )
                        })
                        .await
                        .map_err(|e| anyhow::anyhow!("Task join error: {}", e))?
                    }
                } else {
                    // No PNR filtering needed, just load all files
                    let schema_ref = schema.clone();
                    let base_path_owned = base_path.to_path_buf(); // Create owned copy
                    tokio::task::spawn_blocking(move || {
                        crate::utils::load_parquet_files_parallel(
                            &base_path_owned,
                            Some(schema_ref.as_ref()),
                            None::<&HashSet<String>>, // No filtering
                            None,
                            None,
                        )
                    })
                    .await
                    .map_err(|e| anyhow::anyhow!("Task join error: {}", e))?
                }
            } else {
                // SINGLE FILE HANDLING
                log::info!("Loading from single file: {}", base_path.display());

                // Handle PNR filtering if needed
                if let Some(pnr_filter) = pnr_filter {
                    // If PNR column is available, apply filtering
                    if pnr_column.is_some() {
                        let schema_ref = schema.clone();
                        let pnr_filter = pnr_filter.clone();

                        // Use tokio's spawn_blocking for CPU-intensive work
                        let base_path_owned = base_path.to_path_buf(); // Create owned copy
                        tokio::task::spawn_blocking(move || {
                            crate::utils::read_parquet(
                                &base_path_owned,
                                Some(schema_ref.as_ref()),
                                Some(&pnr_filter),
                                None,
                                None,
                            )
                        })
                        .await
                        .map_err(|e| anyhow::anyhow!("Task join error: {}", e))?
                    } else {
                        // PNR filtering requested but no PNR column available
                        log::warn!(
                            "PNR filtering requested but registry {} doesn't support PNR filtering",
                            self.get_register_name()
                        );

                        // Fall back to loading without filtering
                        let schema_ref = schema.clone();
                        let base_path_owned = base_path.to_path_buf(); // Create owned copy
                        tokio::task::spawn_blocking(move || {
                            crate::utils::read_parquet(
                                &base_path_owned,
                                Some(schema_ref.as_ref()),
                                None::<&HashSet<String>>, // No filtering
                                None,
                                None,
                            )
                        })
                        .await
                        .map_err(|e| anyhow::anyhow!("Task join error: {}", e))?
                    }
                } else {
                    // No PNR filtering needed, just load the file
                    let schema_ref = schema.clone();
                    let base_path_owned = base_path.to_path_buf(); // Create owned copy
                    tokio::task::spawn_blocking(move || {
                        crate::utils::read_parquet(
                            &base_path_owned,
                            Some(schema_ref.as_ref()),
                            None::<&HashSet<String>>, // No filtering
                            None,
                            None,
                        )
                    })
                    .await
                    .map_err(|e| anyhow::anyhow!("Task join error: {}", e))?
                }
            }
        })
    }

    /// Returns whether this registry supports direct PNR filtering
    /// Some registries require joins to filter by PNR
    fn supports_pnr_filter(&self) -> bool {
        self.get_pnr_column_name().is_some()
    }

    /// Returns the column name containing the PNR, if any
    /// Default implementation assumes "PNR" is the column name
    fn get_pnr_column_name(&self) -> Option<&'static str> {
        Some("PNR")
    }

    /// Returns the join column name if this registry needs to be joined with another to get PNRs
    fn get_join_column_name(&self) -> Option<&'static str> {
        None
    }

    /// Enable or disable the unified schema system
    /// Default implementation does nothing, registries that support the unified system
    /// should override this method.
    fn use_unified_system(&mut self, _enable: bool) {
        // Default implementation always uses unified system
    }

    /// Check if the unified schema system is enabled
    /// Default implementation returns true for macro-generated deserializers
    fn is_unified_system_enabled(&self) -> bool {
        true
    }

    /// Helper function to deserialize a batch of records
    /// This is a default implementation for macro-generated deserializers
    fn deserialize_batch(
        &self,
        batch: &RecordBatch,
        deserializer: &dyn crate::registry::trait_deserializer::RegistryDeserializer,
    ) -> crate::error::Result<Vec<crate::models::core::Individual>> {
        deserializer.deserialize_batch(batch)
    }
}

// Registry implementations
pub mod akm;
pub mod bef;
pub mod death {
    pub mod dod;
    pub mod dodsaarsag;
}
pub mod ind;
pub mod lpr;
pub mod mfr;
pub mod uddf;
pub mod vnds;

// Unified registry system support
pub mod factory;
pub mod unified_registry;

mod transform;
pub use transform::{
    add_postal_code_region, add_year_column, filter_by_date_range, filter_out_missing_values,
    map_categorical_values, scale_numeric_values, transform_records,
};

// Centralized registry deserialization
pub mod trait_deserializer;
pub mod trait_deserializer_impl;
pub mod direct_deserializer;
