//! VNDS registry loader implementation
//!
//! The VNDS (Vandringer/Migration) registry contains migration information.

use super::RegisterLoader;
pub mod schema;
pub mod conversion;
use crate::RecordBatch;
use crate::Result;
use crate::async_io::loader::PnrFilterableLoader;
use crate::common::traits::{AsyncDirectoryLoader, AsyncPnrFilterableLoader};
use arrow::datatypes::SchemaRef;
use std::collections::HashSet;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;

/// VNDS registry loader for migration information
/// Implemented using the trait-based approach
#[derive(Debug, Clone)]
pub struct VndsRegister {
    schema: SchemaRef,
    loader: Arc<PnrFilterableLoader>,
}

impl VndsRegister {
    /// Create a new VNDS registry loader
    #[must_use]
    pub fn new() -> Self {
        let schema = schema::vnds_schema();
        let loader = PnrFilterableLoader::with_schema_ref(schema.clone()).with_pnr_column("PNR");

        Self {
            schema,
            loader: Arc::new(loader),
        }
    }
}

impl Default for VndsRegister {
    fn default() -> Self {
        Self::new()
    }
}

impl RegisterLoader for VndsRegister {
    /// Get the name of the register
    fn get_register_name(&self) -> &'static str {
        "VNDS"
    }

    /// Get the schema for this register
    fn get_schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Load records from the VNDS register
    ///
    /// # Arguments
    /// * `base_path` - Base directory containing the VNDS parquet files
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

    /// Load records from the VNDS register asynchronously
    ///
    /// # Arguments
    /// * `base_path` - Base directory containing the VNDS parquet files
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

// Re-export ModelConversion implementation from the conversion module
// The implementation details have been moved there to separate concerns
// and reduce the coupling between registry and model implementations

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    /// Test that the register can be constructed
    #[test]
    fn test_vnds_register_construction() {
        let register = VndsRegister::new();
        assert_eq!(register.get_register_name(), "VNDS");
        assert!(register.supports_pnr_filter());
        assert_eq!(register.get_pnr_column_name(), Some("PNR"));
    }

    /// Test schema initialization
    #[test]
    fn test_schema_initialization() {
        let register = VndsRegister::new();
        let schema = register.get_schema();
        assert!(!schema.fields().is_empty());
    }
    
    /// Test model conversion
    #[test]
    fn test_model_conversion() {
        // TODO: Implement a proper test with a mock batch
        // This would create a test RecordBatch with VNDS data
        // and test the conversion to Individual models
    }
}
