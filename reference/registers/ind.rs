//! IND registry loader implementation
//!
//! The IND (Indkomst) registry contains income and tax information.

use super::RegisterLoader;
use crate::error::Result;
use crate::schema::ind::ind_schema_arc;
use arrow::record_batch::RecordBatch;
use std::collections::HashSet;
use std::path::Path;

/// IND registry loader for income and tax information
pub struct IndRegister;

impl RegisterLoader for IndRegister {
    /// Get the name of the register
    fn get_register_name(&self) -> &'static str {
        "IND"
    }

    /// Load records from the IND register
    ///
    /// # Arguments
    /// * `base_path` - Base directory containing the IND parquet files
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
        let schema = ind_schema_arc();

        // Use the parquet utilities to load IND data
        crate::schema::load_parquet_files_parallel(path, Some(&schema), pnr_filter)
    }
}
