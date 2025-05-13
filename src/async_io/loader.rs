//! Generic Parquet loader implementation
//!
//! This module provides concrete implementations of the async loading traits.

use std::collections::HashSet;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use crate::common::traits::async_loading::AsyncFileHelper;
use crate::common::traits::{
    AsyncDirectoryLoader, AsyncFilterableLoader, AsyncLoader, AsyncPnrFilterableLoader,
};
use crate::error::Result;
use crate::filter::BatchFilter;

/// Standard Parquet loader with configurable schema
///
/// This implementation provides a reusable pattern for loading Parquet files.
#[derive(Debug)]
pub struct ParquetLoader {
    schema: Option<Arc<Schema>>,
    batch_size: Option<usize>,
}

impl ParquetLoader {
    /// Create a new loader with no schema projection
    #[must_use]
    pub const fn new() -> Self {
        Self {
            schema: None,
            batch_size: None,
        }
    }

    /// Create a new loader with schema projection
    #[must_use]
    pub fn with_schema(schema: Schema) -> Self {
        Self {
            schema: Some(Arc::new(schema)),
            batch_size: None,
        }
    }

    /// Create a new loader with schema reference
    #[must_use]
    pub const fn with_schema_ref(schema: SchemaRef) -> Self {
        Self {
            schema: Some(schema),
            batch_size: None,
        }
    }

    /// Set the batch size for loading operations
    #[must_use]
    pub const fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = Some(batch_size);
        self
    }
}

impl Default for ParquetLoader {
    fn default() -> Self {
        Self::new()
    }
}

impl AsyncLoader for ParquetLoader {
    fn load_async<'a>(
        &'a self,
        path: &'a Path,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<RecordBatch>>> + Send + 'a>> {
        let schema = self.schema.as_deref();
        let batch_size = self.batch_size;

        Box::pin(async move { AsyncFileHelper::read_parquet_file(path, schema, batch_size).await })
    }

    fn get_schema(&self) -> Option<Arc<Schema>> {
        self.schema.clone()
    }
}

impl AsyncFilterableLoader for ParquetLoader {
    fn load_with_filter_async<'a>(
        &'a self,
        path: &'a Path,
        filter: Arc<dyn BatchFilter + Send + Sync>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<RecordBatch>>> + Send + 'a>> {
        let batch_size = self.batch_size;

        Box::pin(async move {
            AsyncFileHelper::read_parquet_with_filter(path, filter, batch_size).await
        })
    }
}

impl AsyncDirectoryLoader for ParquetLoader {
    fn find_files_async<'a>(
        &'a self,
        dir: &'a Path,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<PathBuf>>> + Send + 'a>> {
        Box::pin(async move { AsyncFileHelper::find_parquet_files(dir).await })
    }
}

/// Parquet loader with PNR filtering capabilities
#[derive(Debug)]
pub struct PnrFilterableLoader {
    base_loader: ParquetLoader,
    pnr_column: String,
}

impl PnrFilterableLoader {
    /// Create a new PNR-filterable loader
    #[must_use]
    pub fn new() -> Self {
        Self {
            base_loader: ParquetLoader::new(),
            pnr_column: "PNR".to_string(),
        }
    }

    /// Create a new loader with schema projection
    #[must_use]
    pub fn with_schema(schema: Schema) -> Self {
        Self {
            base_loader: ParquetLoader::with_schema(schema),
            pnr_column: "PNR".to_string(),
        }
    }

    /// Create a new loader with schema reference
    #[must_use]
    pub fn with_schema_ref(schema: SchemaRef) -> Self {
        Self {
            base_loader: ParquetLoader::with_schema_ref(schema),
            pnr_column: "PNR".to_string(),
        }
    }

    /// Set the batch size for loading operations
    #[must_use]
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.base_loader = self.base_loader.with_batch_size(batch_size);
        self
    }

    /// Set the PNR column name
    #[must_use]
    pub fn with_pnr_column(mut self, column_name: impl Into<String>) -> Self {
        self.pnr_column = column_name.into();
        self
    }
}

impl Default for PnrFilterableLoader {
    fn default() -> Self {
        Self::new()
    }
}

impl AsyncLoader for PnrFilterableLoader {
    fn load_async<'a>(
        &'a self,
        path: &'a Path,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<RecordBatch>>> + Send + 'a>> {
        self.base_loader.load_async(path)
    }

    fn get_schema(&self) -> Option<Arc<Schema>> {
        self.base_loader.get_schema()
    }
}

impl AsyncFilterableLoader for PnrFilterableLoader {
    fn load_with_filter_async<'a>(
        &'a self,
        path: &'a Path,
        filter: Arc<dyn BatchFilter + Send + Sync>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<RecordBatch>>> + Send + 'a>> {
        self.base_loader.load_with_filter_async(path, filter)
    }
}

impl AsyncDirectoryLoader for PnrFilterableLoader {
    fn find_files_async<'a>(
        &'a self,
        dir: &'a Path,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<PathBuf>>> + Send + 'a>> {
        self.base_loader.find_files_async(dir)
    }
}

impl AsyncPnrFilterableLoader for PnrFilterableLoader {
    fn load_with_pnr_filter_async<'a, S: ::std::hash::BuildHasher + Sync + 'a>(
        &'a self,
        path: &'a Path,
        pnr_filter: Option<&'a HashSet<String, S>>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<RecordBatch>>> + Send + 'a>> {
        // If no PNR filter is provided, use regular loading
        if pnr_filter.is_none() {
            return self.load_async(path);
        }

        // Clone what we need to avoid lifetime issues
        let schema = self.get_schema();
        let batch_size = self.base_loader.batch_size;
        let pnr_column = self.pnr_column.to_string();

        // Create an owned version of the pnr_filter to avoid lifetime issues
        let pnr_filter_owned = pnr_filter.map(|f| {
            let mut owned_set = HashSet::new();
            for item in f {
                owned_set.insert(item.clone());
            }
            owned_set
        });

        Box::pin(async move {
            // First read the file
            let batches = AsyncFileHelper::read_parquet_file(
                path,
                schema.as_ref().map(AsRef::as_ref),
                batch_size,
            )
            .await?;

            // Create a PNR filter with our owned filter
            let filter = Arc::new(crate::filter::pnr::PnrFilter::new(
                &pnr_filter_owned.unwrap(),
                Some(pnr_column),
            ));

            // Apply the filter to each batch
            let mut filtered_batches = Vec::new();

            for batch in batches {
                let filtered_batch = filter.filter(&batch)?;

                // Only add non-empty batches
                if filtered_batch.num_rows() > 0 {
                    filtered_batches.push(filtered_batch);
                }
            }

            Ok(filtered_batches)
        })
    }

    fn get_pnr_column_name(&self) -> Option<&'static str> {
        // Converting String to &'static str isn't directly possible
        // We'll return None for now, as this requires a different approach
        None
    }
}
