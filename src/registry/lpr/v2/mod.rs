//! LPR2 registry loaders using the trait-based approach
//!
//! This module contains trait-based registry loaders for LPR2 (Danish National Patient Registry version 2).

use crate::RecordBatch;
use crate::Result;
use crate::registry::RegisterLoader;
pub mod schema;

use crate::async_io::loader::ParquetLoader;
use crate::common::traits::{AsyncDirectoryLoader, AsyncLoader};

use arrow::datatypes::SchemaRef;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use tokio::fs;

/// Loader for LPR2 Admissions data (`LPR_ADM`)
#[derive(Debug, Clone)]
pub struct LprAdmRegister {
    schema: SchemaRef,
    loader: Arc<ParquetLoader>,
}

impl LprAdmRegister {
    /// Create a new `LPR_ADM` registry loader
    #[must_use]
    pub fn new() -> Self {
        let schema = schema::adm::lpr_adm_schema();
        let loader = ParquetLoader::with_schema_ref(schema.clone());

        Self {
            schema,
            loader: Arc::new(loader),
        }
    }
}

impl Default for LprAdmRegister {
    fn default() -> Self {
        Self::new()
    }
}

impl RegisterLoader for LprAdmRegister {
    fn get_register_name(&self) -> &'static str {
        "lpr_adm"
    }

    fn get_schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn load(
        &self,
        base_path: &Path,
        pnr_filter: Option<&HashSet<String>>,
    ) -> Result<Vec<RecordBatch>> {
        // Create a blocking runtime to run the async code
        let rt = tokio::runtime::Runtime::new()?;

        // Use the trait implementation to load data
        rt.block_on(async {
            if pnr_filter.is_some() {
                log::warn!("PNR filtering not supported for LPR_ADM register");
            }

            // Check if we're dealing with a directory or a single file
            let metadata = fs::metadata(base_path).await.map_err(|e| {
                anyhow::anyhow!("Failed to get metadata for {}: {}", base_path.display(), e)
            })?;

            if metadata.is_dir() {
                // Load all parquet files in the directory
                self.loader.load_directory_async(base_path).await
            } else {
                // Load a single file
                self.loader.load_async(base_path).await
            }
        })
    }

    fn load_async<'a>(
        &'a self,
        base_path: &'a Path,
        pnr_filter: Option<&'a HashSet<String>>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<RecordBatch>>> + Send + 'a>> {
        if pnr_filter.is_some() {
            log::warn!("PNR filtering not supported for LPR_ADM register");
        }

        Box::pin(async move {
            // Check if we're dealing with a directory or a single file
            let metadata = fs::metadata(base_path).await.map_err(|e| {
                anyhow::anyhow!("Failed to get metadata for {}: {}", base_path.display(), e)
            })?;

            if metadata.is_dir() {
                // Load all parquet files in the directory
                self.loader.load_directory_async(base_path).await
            } else {
                // Load a single file
                self.loader.load_async(base_path).await
            }
        })
    }
}

/// Loader for LPR2 Diagnoses data (`LPR_DIAG`)
#[derive(Debug, Clone)]
pub struct LprDiagRegister {
    schema: SchemaRef,
    loader: Arc<ParquetLoader>,
    pub pnr_lookup: Option<HashMap<String, String>>,
}

// Implement StatefulAdapter for LprDiagRegister
impl crate::common::traits::adapter::StatefulAdapter<crate::models::Diagnosis> for LprDiagRegister {
    fn convert_batch(&self, batch: &RecordBatch) -> Result<Vec<crate::models::Diagnosis>> {
        // Delegate to LPR registry for diagnosis conversion
        use crate::common::traits::LprRegistry;
        crate::models::Diagnosis::from_lpr_batch(batch)
    }
}

impl LprDiagRegister {
    /// Create a new `LPR_DIAG` registry loader
    #[must_use]
    pub fn new() -> Self {
        let schema = schema::diag::lpr_diag_schema();
        let loader = ParquetLoader::with_schema_ref(schema.clone());

        Self {
            schema,
            loader: Arc::new(loader),
            pnr_lookup: None,
        }
    }

    /// Create a new `LPR_DIAG` registry loader with a PNR lookup
    #[must_use]
    pub fn with_pnr_lookup(pnr_lookup: HashMap<String, String>) -> Self {
        let schema = schema::diag::lpr_diag_schema();
        let loader = ParquetLoader::with_schema_ref(schema.clone());

        Self {
            schema,
            loader: Arc::new(loader),
            pnr_lookup: Some(pnr_lookup),
        }
    }

    /// Get the PNR lookup for this registry
    #[must_use]
    pub fn get_pnr_lookup(&self) -> Option<HashMap<String, String>> {
        self.pnr_lookup.clone()
    }

    /// Set the PNR lookup for this registry
    pub fn set_pnr_lookup(&mut self, lookup: HashMap<String, String>) {
        self.pnr_lookup = Some(lookup);
    }
}

impl Default for LprDiagRegister {
    fn default() -> Self {
        Self::new()
    }
}

impl RegisterLoader for LprDiagRegister {
    fn get_register_name(&self) -> &'static str {
        "lpr_diag"
    }

    fn get_schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn load(
        &self,
        base_path: &Path,
        _pnr_filter: Option<&HashSet<String>>,
    ) -> Result<Vec<RecordBatch>> {
        // Create a blocking runtime to run the async code
        let rt = tokio::runtime::Runtime::new()?;

        // Use the trait implementation to load data
        rt.block_on(async {
            // Check if we're dealing with a directory or a single file
            let metadata = fs::metadata(base_path).await.map_err(|e| {
                anyhow::anyhow!("Failed to get metadata for {}: {}", base_path.display(), e)
            })?;

            if metadata.is_dir() {
                // Load all parquet files in the directory
                self.loader.load_directory_async(base_path).await
            } else {
                // Load a single file
                self.loader.load_async(base_path).await
            }
        })
    }

    fn load_async<'a>(
        &'a self,
        base_path: &'a Path,
        _pnr_filter: Option<&'a HashSet<String>>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<RecordBatch>>> + Send + 'a>> {
        Box::pin(async move {
            // Check if we're dealing with a directory or a single file
            let metadata = fs::metadata(base_path).await.map_err(|e| {
                anyhow::anyhow!("Failed to get metadata for {}: {}", base_path.display(), e)
            })?;

            if metadata.is_dir() {
                // Load all parquet files in the directory
                self.loader.load_directory_async(base_path).await
            } else {
                // Load a single file
                self.loader.load_async(base_path).await
            }
        })
    }

    fn supports_pnr_filter(&self) -> bool {
        false
    }

    fn get_pnr_column_name(&self) -> Option<&'static str> {
        None
    }

    fn get_join_column_name(&self) -> Option<&'static str> {
        Some("RECNUM")
    }
}

/// Loader for LPR2 Treatments data (`LPR_BES`)
#[derive(Debug, Clone)]
pub struct LprBesRegister {
    schema: SchemaRef,
    loader: Arc<ParquetLoader>,
}

impl LprBesRegister {
    /// Create a new `LPR_BES` registry loader
    #[must_use]
    pub fn new() -> Self {
        let schema = schema::bes::lpr_bes_schema();
        let loader = ParquetLoader::with_schema_ref(schema.clone());

        Self {
            schema,
            loader: Arc::new(loader),
        }
    }
}

impl Default for LprBesRegister {
    fn default() -> Self {
        Self::new()
    }
}

impl RegisterLoader for LprBesRegister {
    fn get_register_name(&self) -> &'static str {
        "lpr_bes"
    }

    fn get_schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn load(
        &self,
        base_path: &Path,
        _pnr_filter: Option<&HashSet<String>>,
    ) -> Result<Vec<RecordBatch>> {
        // Create a blocking runtime to run the async code
        let rt = tokio::runtime::Runtime::new()?;

        // Use the trait implementation to load data
        rt.block_on(async {
            // Check if we're dealing with a directory or a single file
            let metadata = fs::metadata(base_path).await.map_err(|e| {
                anyhow::anyhow!("Failed to get metadata for {}: {}", base_path.display(), e)
            })?;

            if metadata.is_dir() {
                // Load all parquet files in the directory
                self.loader.load_directory_async(base_path).await
            } else {
                // Load a single file
                self.loader.load_async(base_path).await
            }
        })
    }

    fn load_async<'a>(
        &'a self,
        base_path: &'a Path,
        _pnr_filter: Option<&'a HashSet<String>>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<RecordBatch>>> + Send + 'a>> {
        Box::pin(async move {
            // Check if we're dealing with a directory or a single file
            let metadata = fs::metadata(base_path).await.map_err(|e| {
                anyhow::anyhow!("Failed to get metadata for {}: {}", base_path.display(), e)
            })?;

            if metadata.is_dir() {
                // Load all parquet files in the directory
                self.loader.load_directory_async(base_path).await
            } else {
                // Load a single file
                self.loader.load_async(base_path).await
            }
        })
    }

    fn supports_pnr_filter(&self) -> bool {
        false
    }

    fn get_pnr_column_name(&self) -> Option<&'static str> {
        None
    }

    fn get_join_column_name(&self) -> Option<&'static str> {
        Some("RECNUM")
    }
}

// Tests have been moved to the tests directory
// See /tests/registry/lpr/v2/ for the test implementations
