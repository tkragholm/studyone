//! Registry definitions and loaders for various Danish data sources
//!
//! This module contains registry definitions and loaders for various Danish registry data sources.
//! It provides a unified interface for loading and processing parquet files from different
//! registry sources, with optimized loading strategies for each.
//!
//! This module also contains the implementations for converting registry data to domain models,
//! supporting direct model conversion capabilities.
//!
//! Available registries:
//! - AKM (Arbejdsklassifikationsmodulet): Employment information
//! - BEF (Befolkning): Population demographic information
//! - DOD (Deaths): Death records
//! - DODSAARSAG (Causes of death): Death cause information
//! - (Removed IDAN registry)
//! - IND (Indkomst): Income and tax information
//! - LPR (Landspatientregistret): National Patient Registry (versions 2 and 3)
//! - MFR (Medical Birth Registry): Birth information
//! - UDDF (Uddannelse): Educational information
//! - VNDS (Vandringer/Migration): Migration information

use crate::RecordBatch;
use crate::Result;
use crate::common::traits::async_loading::AsyncFilterableLoader;
use crate::common::traits::async_loading::AsyncLoader;
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
    fn load_async<'a>(
        &'a self,
        base_path: &'a Path,
        pnr_filter: Option<&'a HashSet<String>>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<RecordBatch>>> + Send + 'a>> {
        // Default implementation for macro-generated deserializers
        // Get the schema and clone other needed values
        let schema = self.get_schema();

        if let Some(pnr_column) = self.get_pnr_column_name() {
            // Move everything into the async block to avoid local variable references
            Box::pin(async move {
                // Create a loader inside the async block
                let loader = crate::async_io::loader::Loader::with_schema_ref(schema.clone())
                    .with_pnr_column(pnr_column);

                // First check if we're dealing with a directory
                let metadata = tokio::fs::metadata(base_path).await;

                if let Ok(md) = metadata {
                    if md.is_dir() {
                        // Handle directory loading - consistently use DirectoryLoader for better code path
                        if let Some(filter) = pnr_filter {
                            // Create a PNR filter using the expr module
                            use crate::filter::expr::{Expr, ExpressionFilter, LiteralValue};

                            // Create the expression filter using the proper column name
                            let values: Vec<LiteralValue> = filter
                                .iter()
                                .map(|s| LiteralValue::String(s.clone()))
                                .collect();

                            // Create the expression
                            let expr = Expr::In(pnr_column.to_string(), values);
                            
                            // Load with PNR filtering by converting to Express filter
                            let filter = std::sync::Arc::new(ExpressionFilter::new(expr.clone()));
                            
                            // First find all files
                            let parquet_files = crate::common::traits::async_loading::AsyncFileHelper::find_parquet_files(base_path).await?;
                            
                            if parquet_files.is_empty() {
                                log::warn!("No parquet files found in directory: {}", base_path.display());
                                return Ok(Vec::new());
                            }
                            
                            // Then process each file with the filter
                            let mut all_batches = Vec::new();
                            for file_path in parquet_files {
                                let batches = loader.load_with_filter_async(&file_path, filter.clone()).await?;
                                all_batches.extend(batches);
                            }
                            
                            Ok(all_batches)
                        } else {
                            // Load directory without filtering
                            // Use a single approach to find files
                            let parquet_files = crate::common::traits::async_loading::AsyncFileHelper::find_parquet_files(base_path).await?;
                            
                            if parquet_files.is_empty() {
                                log::warn!("No parquet files found in directory: {}", base_path.display());
                                return Ok(Vec::new());
                            }
                            
                            // Then process each file
                            let mut all_batches = Vec::new();
                            for file_path in parquet_files {
                                let batches = loader.load_async(&file_path).await?;
                                all_batches.extend(batches);
                            }
                            
                            Ok(all_batches)
                        }
                    } else {
                        // Handle single file loading
                        if let Some(filter) = pnr_filter {
                            // Create a PNR filter using the expr module
                            use crate::filter::expr::{Expr, ExpressionFilter, LiteralValue};

                            // Create the expression filter using the proper column name
                            let values: Vec<LiteralValue> = filter
                                .iter()
                                .map(|s| LiteralValue::String(s.clone()))
                                .collect();

                            let expr = Expr::In(pnr_column.to_string(), values);
                            let pnr_filter = ExpressionFilter::new(expr);

                            // Use filter with loader for single file
                            loader
                                .load_with_filter_async(base_path, std::sync::Arc::new(pnr_filter))
                                .await
                        } else {
                            // Load without filtering
                            loader.load_async(base_path).await
                        }
                    }
                } else {
                    // Handle path not found or accessible
                    Err(anyhow::anyhow!("Failed to access path: {}", base_path.display()).into())
                }
            })
        } else {
            // If no PNR column is available, use a regular loader
            Box::pin(async move {
                let loader = crate::async_io::loader::Loader::with_schema_ref(schema);

                // First check if we're dealing with a directory
                let metadata = tokio::fs::metadata(base_path).await;

                if let Ok(md) = metadata {
                    if md.is_dir() {
                        // Handle directory loading
                        // Use a single approach to find files
                        let parquet_files = crate::common::traits::async_loading::AsyncFileHelper::find_parquet_files(base_path).await?;
                        
                        if parquet_files.is_empty() {
                            log::warn!("No parquet files found in directory: {}", base_path.display());
                            return Ok(Vec::new());
                        }
                        
                        // Then process each file
                        let mut all_batches = Vec::new();
                        for file_path in parquet_files {
                            let batches = loader.load_async(&file_path).await?;
                            all_batches.extend(batches);
                        }
                        
                        Ok(all_batches)
                    } else {
                        // Load single file
                        loader.load_async(base_path).await
                    }
                } else {
                    // Handle path not found or accessible
                    Err(anyhow::anyhow!("Failed to access path: {}", base_path.display()).into())
                }
            })
        }
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

// Generic deserializer for all registries
pub mod generic_deserializer;

// Unified registry system support
pub mod unified_registry;

// Re-export registry structs for easier access
pub use death::dod::DodRegistry;
pub use death::dodsaarsag::DodsaarsagRegister;
pub use ind::IndRegistry;
pub use lpr::{
    discovery::{LprPaths, find_lpr_files},
    v2::{LprAdmRegister, LprBesRegister, LprDiagRegister},
    v3::{Lpr3DiagnoserRegister, Lpr3KontakterRegister},
};
pub use mfr::MfrRegistry;
pub use uddf::UddfRegistry;
pub use vnds::VndsRegistry;

pub mod factory;

mod transform;
pub use transform::{
    add_postal_code_region, add_year_column, filter_by_date_range, filter_out_missing_values,
    map_categorical_values, scale_numeric_values, transform_records,
};

// Centralized registry deserialization and detection
pub mod deserializer_functions;
pub mod deserializer_macros;
pub mod detect;
pub mod extractors;
pub mod models;
//pub mod registry_macros;
pub mod trait_deserializer;
pub mod trait_deserializer_impl;
