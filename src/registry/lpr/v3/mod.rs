//! LPR3 registry loaders
//!
//! This module contains registry loaders for LPR3 (Danish National Patient Registry version 3).

use crate::RecordBatch;
use crate::Result;
use crate::registry::RegisterLoader;
pub mod schema;
use schema::{lpr3_diagnoser_schema, lpr3_kontakter_schema};

use crate::async_io::parallel_ops::load_parquet_files_parallel_with_pnr_filter_async;
use crate::filter::async_filtering::read_parquet_with_optional_pnr_filter_async;
use crate::load_parquet_files_parallel;

use crate::read_parquet;
use arrow::datatypes::SchemaRef;
use std::collections::HashSet;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;

/// Loader for LPR3 Contacts data (`LPR3_KONTAKTER`)
#[derive(Debug, Clone)]
pub struct Lpr3KontakterRegister {
    schema: SchemaRef,
}

impl Lpr3KontakterRegister {
    /// Create a new `LPR3_KONTAKTER` registry loader
    #[must_use]
    pub fn new() -> Self {
        Self {
            schema: lpr3_kontakter_schema(),
        }
    }
}

impl Default for Lpr3KontakterRegister {
    fn default() -> Self {
        Self::new()
    }
}

impl RegisterLoader for Lpr3KontakterRegister {
    fn get_register_name(&self) -> &'static str {
        "lpr3_kontakter"
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
            load_parquet_files_parallel(
                base_path,
                Some(self.schema.as_ref()),
                pnr_filter_ref,
                None,
                None,
            )
        } else {
            // Try to load a single file
            read_parquet(
                base_path,
                Some(self.schema.as_ref()),
                pnr_filter_ref,
                None,
                None,
            )
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
                    pnr_filter_ref,
                )
                .await
            } else {
                // Try to load a single file
                read_parquet_with_optional_pnr_filter_async(
                    base_path,
                    Some(self.schema.as_ref()),
                    pnr_filter_ref,
                )
                .await
            }
        })
    }
}

/// Loader for LPR3 Diagnoses data (`LPR3_DIAGNOSER`)
#[derive(Debug, Clone)]
pub struct Lpr3DiagnoserRegister {
    schema: SchemaRef,
    pub pnr_lookup: Option<std::collections::HashMap<String, String>>,
}

// Implement StatefulAdapter for Lpr3DiagnoserRegister
impl crate::common::traits::adapter::StatefulAdapter<crate::models::Diagnosis> for Lpr3DiagnoserRegister {
    fn convert_batch(&self, batch: &RecordBatch) -> Result<Vec<crate::models::Diagnosis>> {
        // Delegate to LPR registry for diagnosis conversion
        use crate::common::traits::LprRegistry;
        crate::models::Diagnosis::from_lpr_batch(batch)
    }
}

impl Lpr3DiagnoserRegister {
    /// Create a new `LPR3_DIAGNOSER` registry loader
    #[must_use]
    pub fn new() -> Self {
        Self {
            schema: lpr3_diagnoser_schema(),
            pnr_lookup: None,
        }
    }
}

impl Default for Lpr3DiagnoserRegister {
    fn default() -> Self {
        Self::new()
    }
}

impl RegisterLoader for Lpr3DiagnoserRegister {
    fn get_register_name(&self) -> &'static str {
        "lpr3_diagnoser"
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
            // (because LPR3_DIAGNOSER needs to be linked via DW_EK_KONTAKT)
            let batches = load_parquet_files_parallel::<std::collections::hash_map::RandomState>(
                base_path,
                Some(self.schema.as_ref()),
                None,
                None,
                None,
            )?;
            Ok(batches)
        } else {
            // Try to load a single file
            let batches = read_parquet::<std::collections::hash_map::RandomState>(
                base_path,
                Some(self.schema.as_ref()),
                None,
                None,
                None,
            )?;
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
                let batches = load_parquet_files_parallel_with_pnr_filter_async::<
                    std::collections::hash_map::RandomState,
                >(base_path, Some(self.schema.as_ref()), None)
                .await?;
                Ok(batches)
            } else {
                // Try to load a single file
                let batches = read_parquet_with_optional_pnr_filter_async::<
                    std::collections::hash_map::RandomState,
                >(base_path, Some(self.schema.as_ref()), None)
                .await?;
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
        Some("DW_EK_KONTAKT")
    }
}
