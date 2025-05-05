//! LPR (Landspatientregistret) registry loaders
//!
//! This module contains registry loaders for different versions of the Danish National Patient Registry (LPR).

use super::RegisterLoader;
use super::schemas::lpr_adm::lpr_adm_schema;
use super::schemas::lpr_bes::lpr_bes_schema;
use super::schemas::lpr_diag::lpr_diag_schema;
use super::schemas::lpr3_diagnoser::lpr3_diagnoser_schema;
use super::schemas::lpr3_kontakter::lpr3_kontakter_schema;
use crate::Error;
use crate::RecordBatch;
use crate::Result;

use crate::load_parquet_files_parallel;
use crate::async_io::parallel_ops::load_parquet_files_parallel_with_pnr_filter_async;
use crate::read_parquet;
use crate::async_io::filter_ops::read_parquet_with_optional_pnr_filter_async;
use arrow::datatypes::SchemaRef;
// rayon prelude removed as it's no longer needed
use std::collections::HashSet;
use std::future::Future;
use std::path::{Path, PathBuf};
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

/// Loader for LPR3 Contacts data (`LPR3_KONTAKTER`)
#[derive(Debug, Clone)]
pub struct Lpr3KontakterRegister {
    schema: SchemaRef,
}

impl Lpr3KontakterRegister {
    /// Create a new `LPR3_KONTAKTER` registry loader
    #[must_use] pub fn new() -> Self {
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

/// Loader for LPR3 Diagnoses data (`LPR3_DIAGNOSER`)
#[derive(Debug, Clone)]
pub struct Lpr3DiagnoserRegister {
    schema: SchemaRef,
}

impl Lpr3DiagnoserRegister {
    /// Create a new `LPR3_DIAGNOSER` registry loader
    #[must_use] pub fn new() -> Self {
        Self {
            schema: lpr3_diagnoser_schema(),
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
        Some("DW_EK_KONTAKT")
    }
}

/// Data structure to hold paths to different LPR files
#[derive(Default, Debug, Clone)]
pub struct LprPaths {
    /// Path to `LPR_ADM` files
    pub lpr_adm: Option<PathBuf>,
    /// Path to `LPR_DIAG` files
    pub lpr_diag: Option<PathBuf>,
    /// Path to `LPR_BES` files
    pub lpr_bes: Option<PathBuf>,
    /// Path to `LPR3_KONTAKTER` files
    pub lpr3_kontakter: Option<PathBuf>,
    /// Path to `LPR3_DIAGNOSER` files
    pub lpr3_diagnoser: Option<PathBuf>,
}

/// Find LPR files in a directory
pub fn find_lpr_files(base_dir: &Path) -> Result<LprPaths> {
    if !base_dir.exists() {
        return Err(Error::ValidationError(format!(
            "Base path does not exist: {}",
            base_dir.display()
        ))
        .into());
    }

    let mut lpr_paths = LprPaths::default();

    // Walk the directory to find LPR files
    visit_dirs(base_dir, &mut lpr_paths)?;

    // Validate that we found at least some files
    if lpr_paths.lpr_adm.is_none() && lpr_paths.lpr3_kontakter.is_none() {
        return Err(Error::ValidationError(format!(
            "No LPR files found in directory: {}",
            base_dir.display()
        ))
        .into());
    }

    Ok(lpr_paths)
}

// Helper function to recursively visit directories and find LPR files
fn visit_dirs(dir: &Path, paths: &mut LprPaths) -> Result<()> {
    if !dir.is_dir() {
        return Ok(());
    }

    // Check if this directory contains LPR files
    let dir_name = dir.file_name().and_then(|n| n.to_str()).unwrap_or("");
    let dir_name_lower = dir_name.to_lowercase();

    // Check if directory name matches LPR patterns
    if dir_name_lower.contains("lpr_adm") {
        paths.lpr_adm = Some(dir.to_path_buf());
    } else if dir_name_lower.contains("lpr_diag") {
        paths.lpr_diag = Some(dir.to_path_buf());
    } else if dir_name_lower.contains("lpr_bes") {
        paths.lpr_bes = Some(dir.to_path_buf());
    } else if dir_name_lower.contains("lpr3_kontakter") {
        paths.lpr3_kontakter = Some(dir.to_path_buf());
    } else if dir_name_lower.contains("lpr3_diagnoser") {
        paths.lpr3_diagnoser = Some(dir.to_path_buf());
    } else {
        // Read directory contents
        let entries: Vec<_> = match std::fs::read_dir(dir) {
            Ok(entries) => entries
                .collect::<std::io::Result<Vec<_>>>()
                .map_err(|e| Error::IoError(format!("Failed to read directory: {e}")))?,
            Err(e) => {
                return Err(Error::IoError(format!("Failed to read directory: {e}")).into());
            }
        };

        // Process each entry sequentially to avoid Fn closure issues
        for entry in entries {
            let path = entry.path();

            // Process directories recursively
            if path.is_dir() {
                visit_dirs(&path, paths)?;
                continue;
            }

            // Process files for LPR patterns
            if !path.is_file() {
                continue;
            }

            let file_name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            let file_name_lower = file_name.to_lowercase();
            let parent_path = path.parent().unwrap_or(dir).to_path_buf();

            // Check for LPR patterns
            if file_name_lower.contains("lpr_adm") && path.extension().is_some_and(|ext| ext == "parquet") {
                paths.lpr_adm = Some(parent_path);
            } else if file_name_lower.contains("lpr_diag") && path.extension().is_some_and(|ext| ext == "parquet") {
                paths.lpr_diag = Some(parent_path);
            } else if file_name_lower.contains("lpr_bes") && path.extension().is_some_and(|ext| ext == "parquet") {
                paths.lpr_bes = Some(parent_path);
            } else if file_name_lower.contains("lpr3_kontakter") && path.extension().is_some_and(|ext| ext == "parquet") {
                paths.lpr3_kontakter = Some(parent_path);
            } else if file_name_lower.contains("lpr3_diagnoser") && path.extension().is_some_and(|ext| ext == "parquet") {
                paths.lpr3_diagnoser = Some(parent_path);
            }
        }
    }

    Ok(())
}
