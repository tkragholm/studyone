//! DOD registry loader implementation
//!
//! The DOD registry contains death records.

pub mod schema;
pub mod schema_unified;
pub mod conversion;
pub mod deserializer;
pub mod trait_deserializer;
pub mod individual;
use crate::RecordBatch;
use crate::RegisterLoader;
use crate::Result;
use crate::async_io::parallel_ops::load_parquet_files_parallel_with_pnr_filter_async;
use crate::load_parquet_files_parallel;
use arrow::datatypes::SchemaRef;
use std::collections::HashSet;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;

/// DOD registry loader for death records
#[derive(Debug, Clone)]
pub struct DodRegister {
    schema: SchemaRef,
}

impl DodRegister {
    /// Create a new DOD registry loader
    #[must_use]
    pub fn new() -> Self {
        Self {
            schema: schema::dod_schema(),
        }
    }
}

impl Default for DodRegister {
    fn default() -> Self {
        Self::new()
    }
}

impl RegisterLoader for DodRegister {
    /// Get the name of the register
    fn get_register_name(&self) -> &'static str {
        "DOD"
    }

    /// Get the schema for this register
    fn get_schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Load records from the DOD register
    ///
    /// # Arguments
    /// * `base_path` - Base directory containing the DOD parquet files
    /// * `pnr_filter` - Optional filter to only load data for specific PNRs
    ///
    /// # Returns
    /// * `Result<Vec<RecordBatch>>` - Arrow record batches containing the loaded data
    fn load(
        &self,
        base_path: &Path,
        pnr_filter: Option<&HashSet<String>>,
    ) -> Result<Vec<RecordBatch>> {
        // Use optimized parallel loading for DOD data
        let pnr_filter_arc = pnr_filter.map(|f| std::sync::Arc::new(f.clone()));
        let pnr_filter_ref = pnr_filter_arc.as_ref().map(std::convert::AsRef::as_ref);
        load_parquet_files_parallel(
            base_path,
            Some(self.schema.as_ref()),
            pnr_filter_ref,
            None,
            None,
        )
    }

    /// Load records from the DOD register asynchronously
    ///
    /// # Arguments
    /// * `base_path` - Base directory containing the DOD parquet files
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
            // Use optimized async parallel loading for DOD data
            let pnr_filter_arc = pnr_filter.map(|f| std::sync::Arc::new(f.clone()));
            let pnr_filter_ref = pnr_filter_arc.as_ref().map(std::convert::AsRef::as_ref);
            load_parquet_files_parallel_with_pnr_filter_async(
                base_path,
                Some(self.schema.as_ref()),
                pnr_filter_ref,
            )
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
