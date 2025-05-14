//! Example demonstrating the use of the async loading traits
//!
//! This example shows how to use the async loading traits and implementations
//! to load and filter Parquet files asynchronously.

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use par_reader::async_io::ParquetLoader;
use par_reader::common::traits::{AsyncDirectoryLoader, AsyncLoader, AsyncParallelLoader};
use par_reader::error::Result;
use par_reader::registry::bef::schema;

/// Run the async loader example
pub async fn run_async_loader_example(path: &Path) -> Result<()> {
    // 5. Parallel async loading (if path is a directory)
    if path.is_dir() {
        let parallel_result = parallel_async_loading(path).await?;
        println!(
            "Parallel loading: {} batches with {} total rows",
            parallel_result.len(),
            parallel_result
                .iter()
                .map(arrow::array::RecordBatch::num_rows)
                .sum::<usize>()
        );
    } else {
        println!("Skipping parallel loading test as path is not a directory");
    }

    Ok(())
}

/// Demonstrate parallel loading from multiple sources
async fn parallel_async_loading(path: &Path) -> Result<Vec<RecordBatch>> {
    if !path.is_dir() {
        println!("Path is not a directory");
        return Ok(Vec::new());
    }

    // Create a regular loader for finding files
    let bef_schema = schema::bef_schema();
    let file_finder = ParquetLoader::with_schema_ref(bef_schema.clone());

    // Create two loaders to use with different files (casted to AsyncLoader trait for parallel loading)
    let loader1: Arc<dyn AsyncLoader> =
        Arc::new(ParquetLoader::with_schema_ref(bef_schema.clone()));
    let loader2: Arc<dyn AsyncLoader> = Arc::new(ParquetLoader::with_schema_ref(bef_schema));

    // Find parquet files with the regular loader that has AsyncDirectoryLoader impl
    let files = file_finder.find_files_async(path).await?;

    // If we have at least two files, demonstrate parallel loading
    if files.len() >= 2 {
        // Create a vector of (loader, path) pairs
        let sources: Vec<(Arc<dyn AsyncLoader>, &Path)> =
            vec![(loader1, files[0].as_path()), (loader2, files[1].as_path())];

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

fn main() {
    println!("run_async_loader_example");
}
