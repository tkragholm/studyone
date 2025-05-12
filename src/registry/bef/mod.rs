//! BEF registry loader implementation
//!
//! The BEF (Befolkning) registry contains population demographic information.

mod register;
pub use register::BefCombinedRegister;

pub mod individual;
pub mod schema;
pub mod trait_deserializer;
pub mod trait_deserializer_macro;

use super::RegisterLoader;
use crate::RecordBatch;
use crate::Result;
use crate::async_io::loader::PnrFilterableLoader;
use crate::common::traits::{AsyncDirectoryLoader, AsyncPnrFilterableLoader};
use arrow::datatypes::SchemaRef;
use schema::bef_schema;
use std::collections::HashSet;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;

/// BEF registry loader for population demographic information
/// Implemented using the trait-based approach
#[derive(Debug, Clone)]
pub struct BefRegister {
    schema: SchemaRef,
    loader: Arc<PnrFilterableLoader>,
    unified_system: bool,
}

impl BefRegister {
    /// Create a new BEF registry loader
    #[must_use]
    pub fn new() -> Self {
        let schema = bef_schema();
        let loader = PnrFilterableLoader::with_schema_ref(schema.clone()).with_pnr_column("PNR");

        Self {
            schema,
            loader: Arc::new(loader),
            unified_system: false,
        }
    }

    /// Enable or disable the unified schema system
    pub fn use_unified_system(&mut self, enable: bool) {
        self.unified_system = enable;

        // Update schema based on the unified system setting
        self.schema = if enable {
            schema::bef_schema()
        } else {
            schema::bef_schema()
        };

        // Update the loader with the new schema
        self.loader = Arc::new(
            PnrFilterableLoader::with_schema_ref(self.schema.clone()).with_pnr_column("PNR"),
        );
    }

    /// Check if the unified schema system is enabled
    pub fn is_unified_system_enabled(&self) -> bool {
        self.unified_system
    }
}

impl Default for BefRegister {
    fn default() -> Self {
        Self::new()
    }
}

impl RegisterLoader for BefRegister {
    /// Get the name of the register
    fn get_register_name(&self) -> &'static str {
        "BEF"
    }

    /// Get the schema for this register
    fn get_schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Enable or disable the unified schema system
    fn use_unified_system(&mut self, enable: bool) {
        // Call the struct's own method
        BefRegister::use_unified_system(self, enable);
    }

    /// Check if the unified schema system is enabled
    fn is_unified_system_enabled(&self) -> bool {
        self.unified_system
    }

    /// Load records from the BEF register
    ///
    /// # Arguments
    /// * `base_path` - Base directory containing the BEF parquet files
    /// * `pnr_filter` - Optional filter to only load data for specific PNRs
    ///
    /// # Returns
    /// * `Result<Vec<RecordBatch>>` - Arrow record batches containing the loaded data
    fn load(
        &self,
        base_path: &Path,
        pnr_filter: Option<&HashSet<String>>,
    ) -> Result<Vec<RecordBatch>> {
        // Create a blocking runtime to run the async code
        let rt = tokio::runtime::Runtime::new()?;

        // Use the trait implementation to load data
        rt.block_on(async {
            if let Some(filter) = pnr_filter {
                // Use the PNR filter loader if a filter is provided
                self.loader
                    .load_with_pnr_filter_async(base_path, Some(filter))
                    .await
            } else {
                // Otherwise use the directory loader
                self.loader.load_directory_async(base_path).await
            }
        })
    }

    /// Load records from the BEF register asynchronously
    ///
    /// # Arguments
    /// * `base_path` - Base directory containing the BEF parquet files
    /// * `pnr_filter` - Optional filter to only load data for specific PNRs
    ///
    /// # Returns
    /// * `Result<Vec<RecordBatch>>` - Arrow record batches containing the loaded data
    fn load_async<'a>(
        &'a self,
        base_path: &'a Path,
        pnr_filter: Option<&'a HashSet<String>>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<RecordBatch>>> + Send + 'a>> {
        // Use the trait-based loader directly for async operations
        if let Some(filter) = pnr_filter {
            // Use the PNR filter loader if a filter is provided
            self.loader
                .load_with_pnr_filter_async(base_path, Some(filter))
        } else {
            // Otherwise use the directory loader
            self.loader.load_directory_async(base_path)
        }
    }

    /// Returns whether this registry supports direct PNR filtering
    fn supports_pnr_filter(&self) -> bool {
        true
    }

    /// Returns the column name containing the PNR
    fn get_pnr_column_name(&self) -> Option<&'static str> {
        Some("PNR")
    }
}
