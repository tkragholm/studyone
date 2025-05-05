//! MFR registry loader implementation
//!
//! The MFR (Medical Birth Registry) registry contains birth information.

use super::RegisterLoader;
use super::schemas::mfr::mfr_schema;
use crate::RecordBatch;
use crate::Result;
use crate::load_parquet_files_parallel;
use crate::load_parquet_files_parallel_async;
use arrow::datatypes::SchemaRef;
use std::collections::HashSet;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;

/// MFR registry loader for birth information
#[derive(Debug, Clone)]
pub struct MfrRegister {
    schema: SchemaRef,
}

impl MfrRegister {
    /// Create a new MFR registry loader
    pub fn new() -> Self {
        Self {
            schema: mfr_schema(),
        }
    }
}

impl Default for MfrRegister {
    fn default() -> Self {
        Self::new()
    }
}

impl RegisterLoader for MfrRegister {
    /// Get the name of the register
    fn get_register_name(&self) -> &'static str {
        "MFR"
    }

    /// Get the schema for this register
    fn get_schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Load records from the MFR register
    ///
    /// # Arguments
    /// * `base_path` - Base directory containing the MFR parquet files
    /// * `pnr_filter` - Optional filter to only load data for specific PNRs
    ///
    /// # Returns
    /// * `Result<Vec<RecordBatch>>` - Arrow record batches containing the loaded data
    fn load(
        &self,
        base_path: &Path,
        pnr_filter: Option<&HashSet<String>>,
    ) -> Result<Vec<RecordBatch>> {
        // Use optimized parallel loading for MFR data
        let pnr_filter_arc = pnr_filter.map(|f| std::sync::Arc::new(f.clone()));
        let pnr_filter_ref = pnr_filter_arc.as_ref().map(std::convert::AsRef::as_ref);
        load_parquet_files_parallel(base_path, Some(self.schema.as_ref()), pnr_filter_ref)
    }

    /// Load records from the MFR register asynchronously
    ///
    /// # Arguments
    /// * `base_path` - Base directory containing the MFR parquet files
    /// * `pnr_filter` - Optional filter to only load data for specific PNRs
    ///
    /// # Returns
    /// * `Result<Vec<RecordBatch>>` - Arrow record batches containing the loaded data
    fn load_async<'a>(
        &'a self,
        base_path: &'a Path,
        pnr_filter: Option<&'a HashSet<String>>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<RecordBatch>>> + Send + 'a>> {
        Box::pin(async move {
            // Use optimized async parallel loading for MFR data
            let pnr_filter_arc = pnr_filter.map(|f| std::sync::Arc::new(f.clone()));
            let pnr_filter_ref = pnr_filter_arc.as_ref().map(std::convert::AsRef::as_ref);
            load_parquet_files_parallel_async(base_path, Some(self.schema.as_ref()), pnr_filter_ref)
                .await
        })
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
