//! Async loading trait implementations
//!
//! This module provides standardized traits and implementations for asynchronous
//! data loading operations. It centralizes common async loading patterns to reduce
//! code duplication and standardize error handling.

use std::collections::HashSet;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use futures::stream::{self, StreamExt};
use futures::TryStreamExt;

use crate::error::Result;
use crate::filter::{BatchFilter, Expr};

/// Core trait for asynchronous data loading
pub trait AsyncLoader: Send + Sync {
    /// Load data asynchronously
    ///
    /// This method provides a standardized interface for async loading operations.
    /// Implementations should return a pinned future that resolves to a Result containing
    /// the loaded data.
    fn load_async<'a>(
        &'a self,
        path: &'a Path,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<RecordBatch>>> + Send + 'a>>;
    
    /// Get the Arrow schema for this loader
    fn get_schema(&self) -> Option<Arc<Schema>>;
}

/// Extension trait for async loaders that support filtering
pub trait AsyncFilterableLoader: AsyncLoader {
    /// Load data asynchronously with filtering
    ///
    /// This method extends the basic async loading with filtering capabilities.
    fn load_with_filter_async<'a>(
        &'a self,
        path: &'a Path,
        filter: Arc<dyn BatchFilter + Send + Sync>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<RecordBatch>>> + Send + 'a>>;
    
    /// Load data asynchronously with expression filtering
    fn load_with_expr_async<'a>(
        &'a self,
        path: &'a Path,
        expr: &'a Expr,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<RecordBatch>>> + Send + 'a>> {
        // Default implementation using the filter method
        Box::pin(async move {
            let filter = Arc::new(crate::filter::expr::ExpressionFilter::new(expr.clone()));
            self.load_with_filter_async(path, filter).await
        })
    }
}

/// Extension trait for async loaders that support PNR filtering
pub trait AsyncPnrFilterableLoader: AsyncLoader {
    /// Load data asynchronously with PNR filtering
    ///
    /// This method extends the basic async loading with PNR-specific filtering.
    fn load_with_pnr_filter_async<'a, S: ::std::hash::BuildHasher + Sync + 'a>(
        &'a self,
        path: &'a Path,
        pnr_filter: Option<&'a HashSet<String, S>>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<RecordBatch>>> + Send + 'a>>;
    
    /// Get the PNR column name for this loader
    fn get_pnr_column_name(&self) -> Option<&'static str> {
        Some("PNR")
    }
}

/// Extension trait for async loading from directories
pub trait AsyncDirectoryLoader: AsyncLoader {
    /// Find files to load asynchronously
    ///
    /// This method searches a directory for files matching specific criteria.
    fn find_files_async<'a>(
        &'a self,
        dir: &'a Path,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<PathBuf>>> + Send + 'a>>;
    
    /// Load data from a directory asynchronously
    ///
    /// This method loads all matching files from a directory.
    fn load_directory_async<'a>(
        &'a self,
        dir: &'a Path,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<RecordBatch>>> + Send + 'a>> {
        Box::pin(async move {
            // Find all files in the directory
            let files = self.find_files_async(dir).await?;
            
            // If no files found, return empty result
            if files.is_empty() {
                return Ok(Vec::new());
            }
            
            // Determine optimal parallelism based on CPU count
            let num_cpus = num_cpus::get();
            
            // Process files in optimal batches
            let results = stream::iter(files)
                .map(|path| {
                    let loader = self;
                    async move { loader.load_async(&path).await }
                })
                .buffer_unordered(num_cpus)
                .collect::<Vec<_>>()
                .await;
            
            // Combine all the batches
            let combined_batches = results
                .into_iter()
                .map(|result| match result {
                    Ok(batches) => Ok(batches),
                    Err(e) => {
                        log::error!("Error loading file: {e}");
                        Err(e)
                    }
                })
                .collect::<Result<Vec<Vec<RecordBatch>>>>()?
                .into_iter()
                .flatten()
                .collect();
            
            Ok(combined_batches)
        })
    }
}

/// Trait for loading data from multiple sources in parallel
pub trait AsyncParallelLoader: AsyncLoader {
    /// Load multiple sources in parallel
    ///
    /// This method loads data from multiple sources concurrently.
    fn load_parallel_async<'a>(
        sources: &'a [(Arc<dyn AsyncLoader>, &'a Path)],
    ) -> Pin<Box<dyn Future<Output = Result<Vec<RecordBatch>>> + Send + 'a>> {
        Box::pin(async move {
            // Create futures for each source
            let futures = sources.iter().map(|(loader, path)| {
                let loader = loader.clone();
                let path = path.to_path_buf();
                
                async move {
                    match loader.load_async(&path).await {
                        Ok(batches) => Ok(batches),
                        Err(e) => {
                            log::error!("Error loading from {}: {}", path.display(), e);
                            Err(e)
                        }
                    }
                }
            });
            
            // Execute all futures in parallel
            let results = futures::future::join_all(futures).await;
            
            // Combine results
            let mut combined_batches = Vec::new();
            
            for result in results {
                match result {
                    Ok(batches) => combined_batches.extend(batches),
                    Err(e) => return Err(e),
                }
            }
            
            Ok(combined_batches)
        })
    }
}

/// Helper implementation for async file operations
pub struct AsyncFileHelper;

impl AsyncFileHelper {
    /// Find all Parquet files in a directory asynchronously
    pub async fn find_parquet_files(dir: &Path) -> Result<Vec<PathBuf>> {
        use tokio::fs;
        use crate::utils::{log_operation_start, log_operation_complete, log_warning, validate_directory};
        
        log_operation_start("Searching for parquet files asynchronously in", dir);
        
        // Validate directory
        validate_directory(dir)?;
        
        // Find all parquet files in the directory
        let mut parquet_files = Vec::<PathBuf>::new();
        
        let mut entries = fs::read_dir(dir)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to read directory {}: {}", dir.display(), e))?;
        
        while let Some(entry_result) = entries
            .next_entry()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to read directory entry: {e}"))?
        {
            let path = entry_result.path();
            let metadata = fs::metadata(&path).await.map_err(|e| {
                anyhow::anyhow!("Failed to read metadata for {}: {}", path.display(), e)
            })?;
            
            if metadata.is_file() && path.extension().is_some_and(|ext| ext == "parquet") {
                parquet_files.push(path);
            }
        }
        
        // If no files found, log a warning
        if parquet_files.is_empty() {
            log_warning("No Parquet files found in directory", Some(dir));
        } else {
            log_operation_complete("found", dir, parquet_files.len(), None);
        }
        
        Ok(parquet_files)
    }
    
    /// Read a Parquet file asynchronously
    pub async fn read_parquet_file(
        path: &Path,
        schema: Option<&Schema>,
        batch_size: Option<usize>,
    ) -> Result<Vec<RecordBatch>> {
        use tokio::fs::File;
        use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
        use crate::utils::{
            DEFAULT_BATCH_SIZE, get_batch_size, log_operation_start, log_operation_complete,
        };
        
        let start = std::time::Instant::now();
        log_operation_start("Reading parquet file asynchronously", path);
        
        // Open file asynchronously
        let file = File::open(path)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to open file {}: {}", path.display(), e))?;
        
        // Create the builder
        let mut builder = ParquetRecordBatchStreamBuilder::new(file)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create parquet reader. {}", e))?;
        
        // Apply projection if schema is provided
        if let Some(schema) = schema {
            // Use the common projection helper
            let file_schema = builder.schema();
            let (has_projection, projection_mask) =
                crate::utils::create_projection(schema, file_schema, builder.parquet_schema());
            
            if has_projection {
                builder = builder.with_projection(projection_mask.unwrap());
            }
        }
        
        // Set batch size - use provided, then env var, then default
        let batch_size = batch_size
            .or_else(get_batch_size)
            .unwrap_or(DEFAULT_BATCH_SIZE);
        
        builder = builder.with_batch_size(batch_size);
        
        // Build the stream
        let stream = builder
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to build parquet stream {}", e))?;
        
        // Collect results
        let batches = stream
            .try_collect::<Vec<_>>()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to read record batches {}", e))?;
        
        log_operation_complete("read", path, batches.len(), Some(start.elapsed()));
        
        Ok(batches)
    }
    
    /// Read a Parquet file asynchronously with filtering
    pub async fn read_parquet_with_filter(
        path: &Path,
        filter: Arc<dyn BatchFilter + Send + Sync>,
        batch_size: Option<usize>,
    ) -> Result<Vec<RecordBatch>> {
        use tokio::fs::File;
        use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
        use crate::utils::{DEFAULT_BATCH_SIZE, get_batch_size};
        
        log::info!("Reading and filtering parquet file asynchronously: {}", path.display());
        
        // Open file asynchronously
        let file = File::open(path)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to open file {}: {}", path.display(), e))?;
        
        // Create the builder
        let mut builder = ParquetRecordBatchStreamBuilder::new(file)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create parquet reader. Error: {}", e))?;
        
        // Get columns required by the filter
        let required_columns = filter.required_columns();
        
        // Create the projection from required columns if any
        if !required_columns.is_empty() {
            // Convert set to Schema for our projection helper
            let fields = required_columns
                .iter()
                .map(|name| arrow::datatypes::Field::new(name, arrow::datatypes::DataType::Utf8, true))
                .collect::<Vec<_>>();
            let projected_schema = arrow::datatypes::Schema::new(fields);
            
            // Use our common projection helper
            let file_schema = builder.schema();
            let (has_projection, projection_mask) = crate::utils::create_projection(
                &projected_schema,
                file_schema,
                builder.parquet_schema(),
            );
            
            if has_projection {
                builder = builder.with_projection(projection_mask.unwrap());
            }
        }
        
        // Set batch size
        let batch_size = batch_size
            .or_else(get_batch_size)
            .unwrap_or(DEFAULT_BATCH_SIZE);
        
        builder = builder.with_batch_size(batch_size);
        
        // Build the stream
        let stream = builder
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to build parquet stream. Error: {}", e))?;
        
        // Process the stream with filtering
        let mut results = Vec::new();
        
        tokio::pin!(stream);
        
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result
                .map_err(|e| anyhow::anyhow!("Failed to read record batch. Error: {}", e))?;
            
            // Apply the filter
            let filtered = filter.filter(&batch)?;
            
            // Only add non-empty batches
            if filtered.num_rows() > 0 {
                results.push(filtered);
            }
        }
        
        log::info!(
            "Successfully read and filtered {} batches from {}",
            results.len(),
            path.display()
        );
        
        Ok(results)
    }
}