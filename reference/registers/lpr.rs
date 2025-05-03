//! LPR (Landspatientregistret) registry loaders
//!
//! This module contains registry loaders for different versions of the Danish National Patient Registry (LPR).

use arrow::record_batch::RecordBatch;
use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::error::{IdsError, Result};
use crate::registry::RegisterLoader;
use crate::schema::{load_parquet_files_parallel, read_parquet};
use crate::schema::lpr_adm::lpr_adm_schema_arc;
use crate::schema::lpr_diag::lpr_diag_schema_arc;
use crate::schema::lpr_bes::lpr_bes_schema_arc;
use crate::schema::lpr3_kontakter::lpr3_kontakter_schema_arc;
use crate::schema::lpr3_diagnoser::lpr3_diagnoser_schema_arc;

/// Loader for LPR2 Admissions data (`LPR_ADM`)
pub struct LprAdmRegister;

impl RegisterLoader for LprAdmRegister {
    fn get_register_name(&self) -> &'static str {
        "lpr_adm"
    }
    
    fn load(&self, base_path: &str, pnr_filter: Option<&HashSet<String>>) -> Result<Vec<RecordBatch>> {
        let path = Path::new(base_path);
        let schema = lpr_adm_schema_arc();
        
        if path.is_dir() {
            // Try to load all parquet files in the directory
            load_parquet_files_parallel(path, Some(schema.as_ref()), pnr_filter)
        } else {
            // Try to load a single file
            read_parquet(path, Some(schema.as_ref()), pnr_filter)
        }
    }
}

/// Loader for LPR2 Diagnoses data (`LPR_DIAG`)
pub struct LprDiagRegister;

impl RegisterLoader for LprDiagRegister {
    fn get_register_name(&self) -> &'static str {
        "lpr_diag"
    }
    
    fn load(&self, base_path: &str, _pnr_filter: Option<&HashSet<String>>) -> Result<Vec<RecordBatch>> {
        let path = Path::new(base_path);
        let schema = lpr_diag_schema_arc();
        
        if path.is_dir() {
            // Try to load all parquet files in the directory without PNR filtering
            // (because LPR_DIAG needs to be linked via RECNUM to get the PNR)
            let batches = load_parquet_files_parallel(path, Some(schema.as_ref()), None)?;
            Ok(batches)
        } else {
            // Try to load a single file
            let batches = read_parquet(path, Some(schema.as_ref()), None)?;
            Ok(batches)
        }
    }
}

/// Loader for LPR2 Treatments data (`LPR_BES`)
pub struct LprBesRegister;

impl RegisterLoader for LprBesRegister {
    fn get_register_name(&self) -> &'static str {
        "lpr_bes"
    }
    
    fn load(&self, base_path: &str, _pnr_filter: Option<&HashSet<String>>) -> Result<Vec<RecordBatch>> {
        let path = Path::new(base_path);
        let schema = lpr_bes_schema_arc();
        
        if path.is_dir() {
            // Try to load all parquet files in the directory without PNR filtering
            // (because LPR_BES needs to be linked via RECNUM to get the PNR)
            let batches = load_parquet_files_parallel(path, Some(schema.as_ref()), None)?;
            Ok(batches)
        } else {
            // Try to load a single file
            let batches = read_parquet(path, Some(schema.as_ref()), None)?;
            Ok(batches)
        }
    }
}

/// Loader for LPR3 Contacts data (`LPR3_KONTAKTER`)
pub struct Lpr3KontakterRegister;

impl RegisterLoader for Lpr3KontakterRegister {
    fn get_register_name(&self) -> &'static str {
        "lpr3_kontakter"
    }
    
    fn load(&self, base_path: &str, pnr_filter: Option<&HashSet<String>>) -> Result<Vec<RecordBatch>> {
        let path = Path::new(base_path);
        let schema = lpr3_kontakter_schema_arc();
        
        if path.is_dir() {
            // Try to load all parquet files in the directory
            load_parquet_files_parallel(path, Some(schema.as_ref()), pnr_filter)
        } else {
            // Try to load a single file
            read_parquet(path, Some(schema.as_ref()), pnr_filter)
        }
    }
}

/// Loader for LPR3 Diagnoses data (`LPR3_DIAGNOSER`)
pub struct Lpr3DiagnoserRegister;

impl RegisterLoader for Lpr3DiagnoserRegister {
    fn get_register_name(&self) -> &'static str {
        "lpr3_diagnoser"
    }
    
    fn load(&self, base_path: &str, _pnr_filter: Option<&HashSet<String>>) -> Result<Vec<RecordBatch>> {
        let path = Path::new(base_path);
        let schema = lpr3_diagnoser_schema_arc();
        
        if path.is_dir() {
            // Try to load all parquet files in the directory without PNR filtering
            // (because LPR3_DIAGNOSER needs to be linked via DW_EK_KONTAKT)
            let batches = load_parquet_files_parallel(path, Some(schema.as_ref()), None)?;
            Ok(batches)
        } else {
            // Try to load a single file
            let batches = read_parquet(path, Some(schema.as_ref()), None)?;
            Ok(batches)
        }
    }
}

/// Find LPR files in a directory
pub fn find_lpr_files(base_dir: &str) -> Result<LprPaths> {
    let base_path = Path::new(base_dir);
    
    if !base_path.exists() {
        return Err(IdsError::Validation(format!("Base path does not exist: {base_dir}")));
    }
    
    let mut lpr_paths = LprPaths::default();
    
    // Walk the directory to find LPR files
    visit_dirs(base_path, &mut lpr_paths)?;
    
    // Validate that we found at least some files
    if lpr_paths.lpr_adm.is_none() && lpr_paths.lpr3_kontakter.is_none() {
        return Err(IdsError::Validation(format!(
            "No LPR files found in directory: {base_dir}"
        )));
    }
    
    Ok(lpr_paths)
}

/// Data structure to hold paths to different LPR files
#[derive(Default)]
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

// Helper function to recursively visit directories and find LPR files
fn visit_dirs(dir: &Path, paths: &mut LprPaths) -> Result<()> {
    if dir.is_dir() {
        // Check if this directory contains LPR files
        let dir_name = dir.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("");
        
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
            // Check files in this directory
            for entry in std::fs::read_dir(dir)? {
                let entry = entry?;
                let path = entry.path();
                
                if path.is_dir() {
                    // Recursively check subdirectories
                    visit_dirs(&path, paths)?;
                } else {
                    // Check if file name matches LPR patterns
                    let file_name = path.file_name()
                        .and_then(|n| n.to_str())
                        .unwrap_or("");
                    
                    let file_name_lower = file_name.to_lowercase();
                    
                    if file_name_lower.contains("lpr_adm") && path.extension().is_some_and(|ext| ext == "parquet") {
                        paths.lpr_adm = Some(path.clone());
                    } else if file_name_lower.contains("lpr_diag") && path.extension().is_some_and(|ext| ext == "parquet") {
                        paths.lpr_diag = Some(path.clone());
                    } else if file_name_lower.contains("lpr_bes") && path.extension().is_some_and(|ext| ext == "parquet") {
                        paths.lpr_bes = Some(path.clone());
                    } else if file_name_lower.contains("lpr3_kontakter") && path.extension().is_some_and(|ext| ext == "parquet") {
                        paths.lpr3_kontakter = Some(path.clone());
                    } else if file_name_lower.contains("lpr3_diagnoser") && path.extension().is_some_and(|ext| ext == "parquet") {
                        paths.lpr3_diagnoser = Some(path.clone());
                    }
                }
            }
        }
    }
    
    Ok(())
}