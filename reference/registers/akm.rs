//! AKM registry loader implementation
//!
//! The AKM (Arbejdsklassifikationsmodulet) registry contains employment information.

use super::RegisterLoader;
use crate::error::Result;
use crate::schema::akm::akm_schema_arc;
use arrow::record_batch::RecordBatch;
use std::collections::HashSet;
use std::path::Path;

/// AKM registry loader for employment information
pub struct AkmRegister;

impl RegisterLoader for AkmRegister {
    /// Get the name of the register
    fn get_register_name(&self) -> &'static str {
        "AKM"
    }

    /// Load records from the AKM register
    ///
    /// # Arguments
    /// * `base_path` - Base directory containing the AKM parquet files
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
        let schema = akm_schema_arc();

        // Use the parquet utilities to load AKM data
        crate::schema::load_parquet_files_parallel(path, Some(&schema), pnr_filter)
    }
}
