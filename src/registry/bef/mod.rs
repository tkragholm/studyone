//! BEF registry loader implementation
//!
//! The BEF (Befolkning) registry contains population demographic information.

mod register;
pub use register::BefCombinedRegister;

pub mod schema;
pub mod conversion;

use super::RegisterLoader;
use schema::bef_schema;
use crate::RecordBatch;
use crate::Result;
use crate::common::traits::{
    AsyncDirectoryLoader, AsyncPnrFilterableLoader
};
use crate::async_io::loader::PnrFilterableLoader;
use arrow::datatypes::SchemaRef;
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
}

impl BefRegister {
    /// Create a new BEF registry loader
    #[must_use]
    pub fn new() -> Self {
        let schema = bef_schema();
        let loader = PnrFilterableLoader::with_schema_ref(schema.clone())
            .with_pnr_column("PNR");
        
        Self {
            schema,
            loader: Arc::new(loader),
        }
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
                self.loader.load_with_pnr_filter_async(base_path, Some(filter)).await
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
            self.loader.load_with_pnr_filter_async(base_path, Some(filter))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter::{Expr, FilterBuilder};
    use std::path::PathBuf;

    #[tokio::test]
    async fn test_bef_basic_loading() -> Result<()> {
        let register = BefRegister::new();
        let test_path = PathBuf::from("test_data/bef");
        
        let result = register.load_async(&test_path, None).await?;
        
        println!("Loaded {} batches from BEF register", result.len());
        println!("Total rows: {}", result.iter().map(|b| b.num_rows()).sum::<usize>());
        
        Ok(())
    }
    
    #[tokio::test]
    async fn test_bef_filtering() -> Result<()> {
        let register = BefRegister::new();
        let test_path = PathBuf::from("test_data/bef");
        
        // Create an expression filter
        let age_filter = Expr::Gt("AGE".to_string(), 18.into());
        let gender_filter = Expr::Eq("GENDER".to_string(), "F".into());
        
        // Combine filters
        let combined_expr = FilterBuilder::from_expr(age_filter)
            .and_expr(gender_filter)
            .build();
        
        // Use the filter with the loader
        let filtered_data = register
            .loader
            .load_with_expr_async(&test_path, &combined_expr)
            .await?;
            
        println!("Filtered data has {} rows", filtered_data.iter().map(|b| b.num_rows()).sum::<usize>());
        
        Ok(())
    }
}