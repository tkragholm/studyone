//! IND registry loader implementation using the trait-based approach
//!
//! The IND (Indkomst) registry contains income and tax information.
//! This implementation uses the new async trait-based system.

use super::RegisterLoader;
use super::schemas::ind::ind_schema;
use crate::RecordBatch;
use crate::Result;
use crate::common::traits::{
    AsyncLoader, AsyncDirectoryLoader, AsyncPnrFilterableLoader, AsyncFilterableLoader
};
use crate::async_io::loader::PnrFilterableLoader;
use crate::filter::Expr;
use arrow::datatypes::SchemaRef;
use std::collections::HashSet;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;

/// IND registry loader for income and tax information
/// Implemented using the new trait-based approach
#[derive(Debug, Clone)]
pub struct IndRegister {
    schema: SchemaRef,
    loader: Arc<PnrFilterableLoader>,
    year: Option<i32>,
}

impl IndRegister {
    /// Create a new IND registry loader
    #[must_use]
    pub fn new() -> Self {
        let schema = ind_schema();
        let loader = PnrFilterableLoader::with_schema_ref(schema.clone())
            .with_pnr_column("PNR");
        
        Self {
            schema,
            loader: Arc::new(loader),
            year: None,
        }
    }
    
    /// Create a new IND registry loader for a specific year
    #[must_use]
    pub fn for_year(year: i32) -> Self {
        let schema = ind_schema();
        let loader = PnrFilterableLoader::with_schema_ref(schema.clone())
            .with_pnr_column("PNR");
        
        Self {
            schema,
            loader: Arc::new(loader),
            year: Some(year),
        }
    }
    
    /// Get the configured year, if any
    #[must_use]
    pub fn year(&self) -> Option<i32> {
        self.year
    }
}

impl Default for IndRegister {
    fn default() -> Self {
        Self::new()
    }
}

impl RegisterLoader for IndRegister {
    /// Get the name of the register
    fn get_register_name(&self) -> &'static str {
        "IND"
    }

    /// Get the schema for this register
    fn get_schema(&self) -> SchemaRef {
        self.schema.clone()
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
        base_path: &Path,
        pnr_filter: Option<&HashSet<String>>,
    ) -> Result<Vec<RecordBatch>> {
        // Create a blocking runtime to run the async code
        let rt = tokio::runtime::Runtime::new()?;
        
        // Use the trait implementation to load data
        rt.block_on(async {
            let result = if let Some(filter) = pnr_filter {
                // Use the PNR filter loader if a filter is provided
                self.loader.load_with_pnr_filter_async(base_path, Some(filter)).await?
            } else {
                // Otherwise use the directory loader
                self.loader.load_directory_async(base_path).await?
            };
            
            // Apply year filter if year is configured
            if let Some(year) = self.year {
                // Create a year filter expression
                let year_filter = Arc::new(
                    crate::filter::expr::ExpressionFilter::new(
                        Expr::Eq("YEAR".to_string(), year.into())
                    )
                );
                
                // Apply the filter to each batch
                let mut filtered_results = Vec::new();
                
                for batch in result {
                    let filtered_batch = year_filter.filter(&batch)?;
                    if filtered_batch.num_rows() > 0 {
                        filtered_results.push(filtered_batch);
                    }
                }
                
                Ok(filtered_results)
            } else {
                Ok(result)
            }
        })
    }

    /// Load records from the IND register asynchronously
    ///
    /// # Arguments
    /// * `base_path` - Base directory containing the IND parquet files
    /// * `pnr_filter` - Optional filter to only load data for specific PNRs
    ///
    /// # Returns
    /// * `Result<Vec<RecordBatch>>` - Arrow record batches containing the loaded data
    fn load_async<'a>(
        &'a self,
        base_path: &'a Path,
        pnr_filter: Option<&'a HashSet<String>>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<RecordBatch>>> + Send + 'a>> {
        // Get references to what we need
        let year = self.year;
        
        // First load the data using the trait-based loader
        let loader_future = if let Some(filter) = pnr_filter {
            self.loader.load_with_pnr_filter_async(base_path, Some(filter))
        } else {
            self.loader.load_directory_async(base_path)
        };
        
        // Create a future that will apply year filtering if needed
        Box::pin(async move {
            let result = loader_future.await?;
            
            // Apply year filter if year is configured
            if let Some(year) = year {
                // Create a year filter expression
                let year_filter = Arc::new(
                    crate::filter::expr::ExpressionFilter::new(
                        Expr::Eq("YEAR".to_string(), year.into())
                    )
                );
                
                // Apply the filter to each batch
                let mut filtered_results = Vec::new();
                
                for batch in result {
                    let filtered_batch = year_filter.filter(&batch)?;
                    if filtered_batch.num_rows() > 0 {
                        filtered_results.push(filtered_batch);
                    }
                }
                
                Ok(filtered_results)
            } else {
                Ok(result)
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter::{Expr, FilterBuilder};
    use std::path::PathBuf;

    #[tokio::test]
    async fn test_ind_with_year_filter() -> Result<()> {
        let register = IndRegister::for_year(2018);
        let test_path = PathBuf::from("test_data/ind");
        
        let result = register.load_async(&test_path, None).await?;
        
        println!("Loaded {} batches for year 2018", result.len());
        println!("Total rows: {}", result.iter().map(|b| b.num_rows()).sum::<usize>());
        
        Ok(())
    }
    
    #[tokio::test]
    async fn test_ind_with_complex_filters() -> Result<()> {
        let register = IndRegister::new();
        let test_path = PathBuf::from("test_data/ind");
        
        // Create a complex filter: income > 500000 AND age < 65
        let income_filter = Expr::Gt("PERINDKIALT".to_string(), 500000.into());
        let age_filter = Expr::Lt("ALDER".to_string(), 65.into());
        
        // Combine filters
        let combined_expr = FilterBuilder::from_expr(income_filter)
            .and_expr(age_filter)
            .build();
        
        // Convert to a batch filter
        let filter = Arc::new(crate::filter::expr::ExpressionFilter::new(combined_expr));
        
        // Use the filter with the loader
        let filtered_data = register
            .loader
            .load_with_filter_async(&test_path, filter)
            .await?;
        
        println!("Filtered income data has {} rows", 
                filtered_data.iter().map(|b| b.num_rows()).sum::<usize>());
        
        Ok(())
    }
}