//! Registry implementation for IDAN (Danish employment statistics)

use crate::error::Result;
use crate::schema::idan::idan_schema_arc;
use crate::schema::parquet_utils::load_parquet_files_parallel;
use arrow::record_batch::RecordBatch;
use std::collections::HashSet;
use std::path::Path;

use super::RegisterLoader;

/// IDAN register loader
#[derive(Debug, Default)]
pub struct IdanRegister;

impl RegisterLoader for IdanRegister {
    fn get_register_name(&self) -> &'static str {
        "IDAN"
    }
    
    fn load(&self, base_path: &str, pnr_filter: Option<&HashSet<String>>) -> Result<Vec<RecordBatch>> {
        let path = Path::new(base_path);
        let schema = idan_schema_arc();
        
        // Load parquet files in parallel
        let records = load_parquet_files_parallel(path, Some(&schema), pnr_filter)?;
        
        Ok(records)
    }
}