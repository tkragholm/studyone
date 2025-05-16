//! LPR file discovery utilities
//!
//! This module contains utilities for discovering LPR files in file systems.

use crate::error::{Error, Result};
use std::path::{Path, PathBuf};

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
            if file_name_lower.contains("lpr_adm")
                && path.extension().is_some_and(|ext| ext == "parquet")
            {
                paths.lpr_adm = Some(parent_path);
            } else if file_name_lower.contains("lpr_diag")
                && path.extension().is_some_and(|ext| ext == "parquet")
            {
                paths.lpr_diag = Some(parent_path);
            } else if file_name_lower.contains("lpr_bes")
                && path.extension().is_some_and(|ext| ext == "parquet")
            {
                paths.lpr_bes = Some(parent_path);
            } else if file_name_lower.contains("lpr3_kontakter")
                && path.extension().is_some_and(|ext| ext == "parquet")
            {
                paths.lpr3_kontakter = Some(parent_path);
            } else if file_name_lower.contains("lpr3_diagnoser")
                && path.extension().is_some_and(|ext| ext == "parquet")
            {
                paths.lpr3_diagnoser = Some(parent_path);
            }
        }
    }

    Ok(())
}