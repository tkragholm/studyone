//! MFR registry loader implementation
//!
//! The MFR (Medicinal Fødselsregister) registry contains information about births.

use super::RegisterLoader;
use crate::error::Result;
use crate::schema::mfr::mfr_schema_arc;
use arrow::record_batch::RecordBatch;
use std::collections::HashSet;
use std::path::Path;

/// MFR registry loader for birth information
pub struct MfrRegister;

impl RegisterLoader for MfrRegister {
    /// Get the name of the register
    fn get_register_name(&self) -> &'static str {
        "MFR"
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
        base_path: &str,
        pnr_filter: Option<&HashSet<String>>,
    ) -> Result<Vec<RecordBatch>> {
        let path = Path::new(base_path);
        let schema = mfr_schema_arc();

        // Use the parquet utilities to load MFR data
        crate::schema::load_parquet_files_parallel(path, Some(&schema), pnr_filter)
    }
}