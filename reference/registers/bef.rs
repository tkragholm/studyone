//! BEF registry loader implementation
//!
//! The BEF (Befolkning) registry contains population demographic information.

use super::RegisterLoader;
use crate::error::Result;
use crate::schema::bef::bef_schema_arc;
use arrow::record_batch::RecordBatch;
use std::collections::HashSet;
use std::path::Path;

/// BEF registry loader for population demographic information
pub struct BefRegister;

impl RegisterLoader for BefRegister {
    /// Get the name of the register
    fn get_register_name(&self) -> &'static str {
        "BEF"
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
        base_path: &str,
        pnr_filter: Option<&HashSet<String>>,
    ) -> Result<Vec<RecordBatch>> {
        let path = Path::new(base_path);
        let schema = bef_schema_arc();

        // Use the parquet utilities to load BEF data
        crate::schema::load_parquet_files_parallel(path, Some(&schema), pnr_filter)
    }
}
