//! Example demonstrating the use of the async loading traits
//!
//! This example shows how to use the async loading traits and implementations
//! to load and filter Parquet files asynchronously.

use std::collections::HashSet;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use crate::async_io::{ParquetLoader, PnrFilterableLoader};
use crate::common::traits::{
    AsyncDirectoryLoader, AsyncFilterableLoader, AsyncLoader, AsyncParallelLoader,
    AsyncPnrFilterableLoader,
};
use crate::error::Result;
use crate::filter::expr::{Expr, LiteralValue};
use crate::registry::schemas::bef::bef_schema;

/// Run the async loader example
pub async fn run_async_loader_example(path: &Path) -> Result<()> {
    println!("Running async loader example with path: {}", path.display());

    // 1. Basic async loading
    let basic_result = basic_async_loading(path).await?;
    println!(
        "Basic loading: {} batches with {} total rows",
        basic_result.len(),
        basic_result.iter().map(|b| b.num_rows()).sum::<usize>()
    );

    // 2. Filtered async loading
    let filtered_result = filtered_async_loading(path).await?;
    println!(
        "Filtered loading: {} batches with {} total rows",
        filtered_result.len(),
        filtered_result.iter().map(|b| b.num_rows()).sum::<usize>()
    );

    // 3. PNR filtered async loading
    let pnr_filtered_result = pnr_filtered_async_loading(path).await?;
    println!(
        "PNR filtered loading: {} batches with {} total rows",
        pnr_filtered_result.len(),
        pnr_filtered_result
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>()
    );

    // 4. Directory async loading
    if path.is_dir() {
        let directory_result = directory_async_loading(path).await?;
        println!(
            "Directory loading: {} batches with {} total rows",
            directory_result.len(),
            directory_result.iter().map(|b| b.num_rows()).sum::<usize>()
        );
    } else {
        println!("Skipping directory loading test as path is not a directory");
    }

    // 5. Parallel async loading (if path is a directory)
    if path.is_dir() {
        let parallel_result = parallel_async_loading(path).await?;
        println!(
            "Parallel loading: {} batches with {} total rows",
            parallel_result.len(),
            parallel_result.iter().map(|b| b.num_rows()).sum::<usize>()
        );
    } else {
        println!("Skipping parallel loading test as path is not a directory");
    }

    Ok(())
}

/// Demonstrate basic async loading
async fn basic_async_loading(path: &Path) -> Result<Vec<RecordBatch>> {
    // Create a basic loader with a schema
    let schema = bef_schema();
    let loader = ParquetLoader::with_schema_ref(schema);

    // Load a file asynchronously
    if path.is_file() {
        loader.load_async(path).await
    } else if path.is_dir() {
        // Find the first parquet file in the directory
        let files = loader.find_files_async(path).await?;
        if let Some(first_file) = files.first() {
            loader.load_async(first_file).await
        } else {
            println!("No parquet files found in directory");
            Ok(Vec::new())
        }
    } else {
        println!("Path is neither a file nor a directory");
        Ok(Vec::new())
    }
}

/// Demonstrate filtered async loading
async fn filtered_async_loading(path: &Path) -> Result<Vec<RecordBatch>> {
    // Create a basic loader with a schema
    let schema = bef_schema();
    let loader = ParquetLoader::with_schema_ref(schema);

    // Create a filter expression (e.g. find individuals with birth year >= 2000)
    let filter_expr = Expr::Gt("FOEDT".to_string(), LiteralValue::Int(20000101));

    // Load a file asynchronously with filtering
    if path.is_file() {
        loader.load_with_expr_async(path, &filter_expr).await
    } else if path.is_dir() {
        // Find the first parquet file in the directory
        let files = loader.find_files_async(path).await?;
        if let Some(first_file) = files.first() {
            loader.load_with_expr_async(first_file, &filter_expr).await
        } else {
            println!("No parquet files found in directory");
            Ok(Vec::new())
        }
    } else {
        println!("Path is neither a file nor a directory");
        Ok(Vec::new())
    }
}

/// Demonstrate PNR filtered async loading
async fn pnr_filtered_async_loading(path: &Path) -> Result<Vec<RecordBatch>> {
    // Create a PNR filterable loader with a schema
    let schema = bef_schema();
    let loader = PnrFilterableLoader::with_schema_ref(schema)
        .with_pnr_column("PNR")
        .with_batch_size(1000);

    // Create a set of PNRs to filter by (for example purposes)
    let mut pnr_filter = HashSet::new();
    pnr_filter.insert("0101606428".to_string());
    pnr_filter.insert("0102606428".to_string());
    pnr_filter.insert("0103606428".to_string());

    // Load a file asynchronously with PNR filtering
    if path.is_file() {
        loader
            .load_with_pnr_filter_async(path, Some(&pnr_filter))
            .await
    } else if path.is_dir() {
        // Find the first parquet file in the directory
        let files = loader.find_files_async(path).await?;
        if let Some(first_file) = files.first() {
            loader
                .load_with_pnr_filter_async(first_file, Some(&pnr_filter))
                .await
        } else {
            println!("No parquet files found in directory");
            Ok(Vec::new())
        }
    } else {
        println!("Path is neither a file nor a directory");
        Ok(Vec::new())
    }
}

/// Demonstrate loading from a directory
async fn directory_async_loading(path: &Path) -> Result<Vec<RecordBatch>> {
    // Create a loader with a schema
    let schema = bef_schema();
    let loader = ParquetLoader::with_schema_ref(schema);

    // Load all files from a directory
    if path.is_dir() {
        loader.load_directory_async(path).await
    } else {
        println!("Path is not a directory");
        Ok(Vec::new())
    }
}

/// Demonstrate parallel loading from multiple sources
async fn parallel_async_loading(path: &Path) -> Result<Vec<RecordBatch>> {
    if !path.is_dir() {
        println!("Path is not a directory");
        return Ok(Vec::new());
    }

    // Create a regular loader for finding files
    let bef_schema = bef_schema();
    let file_finder = ParquetLoader::with_schema_ref(bef_schema.clone());
    
    // Create two loaders to use with different files (casted to AsyncLoader trait for parallel loading)
    let loader1: Arc<dyn AsyncLoader> = Arc::new(ParquetLoader::with_schema_ref(bef_schema.clone()));
    let loader2: Arc<dyn AsyncLoader> = Arc::new(ParquetLoader::with_schema_ref(bef_schema));

    // Find parquet files with the regular loader that has AsyncDirectoryLoader impl
    let files = file_finder.find_files_async(path).await?;

    // If we have at least two files, demonstrate parallel loading
    if files.len() >= 2 {
        // Create a vector of (loader, path) pairs
        let sources: Vec<(Arc<dyn AsyncLoader>, &Path)> = vec![
            (loader1, files[0].as_path()),
            (loader2, files[1].as_path()),
        ];

        // Since AsyncParallelLoader is a trait, we need a concrete type to call
        // its methods. We'll create a helper struct for this.
        struct ParallelLoader;
        
        impl AsyncLoader for ParallelLoader {
            fn load_async<'a>(
                &'a self,
                _path: &'a Path,
            ) -> Pin<Box<dyn Future<Output = Result<Vec<RecordBatch>>> + Send + 'a>> {
                // This is just a placeholder since we don't actually use it
                Box::pin(async { Ok(Vec::new()) })
            }
            
            fn get_schema(&self) -> Option<Arc<Schema>> {
                None
            }
        }
        
        impl AsyncParallelLoader for ParallelLoader {}
        
        // Now use our helper struct to call the trait method
        <ParallelLoader as AsyncParallelLoader>::load_parallel_async(&sources).await
    } else {
        println!("Not enough parquet files found for parallel loading example");
        Ok(Vec::new())
    }
}
