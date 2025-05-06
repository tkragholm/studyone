//! LPR2 registry loaders
//!
//! This module contains registry loaders for LPR2 (Danish National Patient Registry version 2).

use crate::registry::RegisterLoader;
use crate::registry::schemas::lpr_adm::lpr_adm_schema;
use crate::registry::schemas::lpr_bes::lpr_bes_schema;
use crate::registry::schemas::lpr_diag::lpr_diag_schema;
use crate::Error;
use crate::RecordBatch;
use crate::Result;

use crate::load_parquet_files_parallel;
use crate::async_io::parallel_ops::load_parquet_files_parallel_with_pnr_filter_async;
use crate::read_parquet;
use crate::filter::async_filtering::read_parquet_with_optional_pnr_filter_async;
use arrow::datatypes::SchemaRef;
use std::collections::HashSet;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;

/// Loader for LPR2 Admissions data (`LPR_ADM`)
#[derive(Debug, Clone)]
pub struct LprAdmRegister {
    schema: SchemaRef,
}

impl LprAdmRegister {
    /// Create a new `LPR_ADM` registry loader
    #[must_use] pub fn new() -> Self {
        Self {
            schema: lpr_adm_schema(),
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
        let pnr_filter_arc = pnr_filter.map(|f| std::sync::Arc::new(f.clone()));
        let pnr_filter_ref = pnr_filter_arc.as_ref().map(std::convert::AsRef::as_ref);

        if base_path.is_dir() {
            // Try to load all parquet files in the directory
            load_parquet_files_parallel(base_path, Some(self.schema.as_ref()), pnr_filter_ref)
        } else {
            // Try to load a single file
            read_parquet(base_path, Some(self.schema.as_ref()), pnr_filter_ref)
        }
    }

    fn load_async<'a>(
        &'a self,
        base_path: &'a Path,
        pnr_filter: Option<&'a HashSet<String>>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<RecordBatch>>> + Send + 'a>> {
        Box::pin(async move {
            let pnr_filter_arc = pnr_filter.map(|f| std::sync::Arc::new(f.clone()));
            let pnr_filter_ref = pnr_filter_arc.as_ref().map(std::convert::AsRef::as_ref);

            if base_path.is_dir() {
                // Try to load all parquet files in the directory
                load_parquet_files_parallel_with_pnr_filter_async(
                    base_path,
                    Some(self.schema.as_ref()),
                    pnr_filter_ref
                )
                .await
            } else {
                // Try to load a single file
                read_parquet_with_optional_pnr_filter_async(base_path, Some(self.schema.as_ref()), pnr_filter_ref).await
            }
        })
    }
}

/// Loader for LPR2 Diagnoses data (`LPR_DIAG`)
#[derive(Debug, Clone)]
pub struct LprDiagRegister {
    schema: SchemaRef,
}

impl LprDiagRegister {
    /// Create a new `LPR_DIAG` registry loader
    #[must_use] pub fn new() -> Self {
        Self {
            schema: lpr_diag_schema(),
        }
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
        if base_path.is_dir() {
            // Try to load all parquet files in the directory without PNR filtering
            // (because LPR_DIAG needs to be linked via RECNUM to get the PNR)
            let batches = load_parquet_files_parallel::<std::collections::hash_map::RandomState>(base_path, Some(self.schema.as_ref()), None)?;
            Ok(batches)
        } else {
            // Try to load a single file
            let batches = read_parquet::<std::collections::hash_map::RandomState>(base_path, Some(self.schema.as_ref()), None)?;
            Ok(batches)
        }
    }

    fn load_async<'a>(
        &'a self,
        base_path: &'a Path,
        _pnr_filter: Option<&'a HashSet<String>>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<RecordBatch>>> + Send + 'a>> {
        Box::pin(async move {
            if base_path.is_dir() {
                // Try to load all parquet files in the directory without PNR filtering
                let batches =
                    load_parquet_files_parallel_with_pnr_filter_async::<std::collections::hash_map::RandomState>(base_path, Some(self.schema.as_ref()), None)
                        .await?;
                Ok(batches)
            } else {
                // Try to load a single file
                let batches =
                    read_parquet_with_optional_pnr_filter_async::<std::collections::hash_map::RandomState>(base_path, Some(self.schema.as_ref()), None).await?;
                Ok(batches)
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
}

impl LprBesRegister {
    /// Create a new `LPR_BES` registry loader
    #[must_use] pub fn new() -> Self {
        Self {
            schema: lpr_bes_schema(),
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
        if base_path.is_dir() {
            // Try to load all parquet files in the directory without PNR filtering
            // (because LPR_BES needs to be linked via RECNUM to get the PNR)
            let batches = load_parquet_files_parallel::<std::collections::hash_map::RandomState>(base_path, Some(self.schema.as_ref()), None)?;
            Ok(batches)
        } else {
            // Try to load a single file
            let batches = read_parquet::<std::collections::hash_map::RandomState>(base_path, Some(self.schema.as_ref()), None)?;
            Ok(batches)
        }
    }

    fn load_async<'a>(
        &'a self,
        base_path: &'a Path,
        _pnr_filter: Option<&'a HashSet<String>>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<RecordBatch>>> + Send + 'a>> {
        Box::pin(async move {
            if base_path.is_dir() {
                // Try to load all parquet files in the directory without PNR filtering
                let batches =
                    load_parquet_files_parallel_with_pnr_filter_async::<std::collections::hash_map::RandomState>(base_path, Some(self.schema.as_ref()), None)
                        .await?;
                Ok(batches)
            } else {
                // Try to load a single file
                let batches =
                    read_parquet_with_optional_pnr_filter_async::<std::collections::hash_map::RandomState>(base_path, Some(self.schema.as_ref()), None).await?;
                Ok(batches)
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