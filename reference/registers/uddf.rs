//! UDDF registry loader implementation
//!
//! The UDDF (Uddannelse) registry contains educational information.

use super::RegisterLoader;
use crate::error::Result;
use crate::schema::uddf::uddf_schema_arc;
use arrow::record_batch::RecordBatch;
use std::collections::HashSet;
use std::path::Path;

/// UDDF registry loader for educational information
pub struct UddfRegister;

impl RegisterLoader for UddfRegister {
    /// Get the name of the register
    fn get_register_name(&self) -> &'static str {
        "UDDF"
    }

    /// Load records from the UDDF register
    ///
    /// # Arguments
    /// * `base_path` - Base directory containing the UDDF parquet files
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
        let schema = uddf_schema_arc();

        // Use the parquet utilities to load UDDF data
        crate::schema::load_parquet_files_parallel(path, Some(&schema), pnr_filter)
    }
}
