//! General path utilities for working with registry files
//!
//! This module provides utilities for working with file paths and directories
//! common to all registry types.

use std::path::PathBuf;

/// Get base directory for registry data
///
/// This function returns the base directory for registry data based on the
/// operating system. It should be used for tests only, not in production code.
///
/// # Returns
/// The base directory for registry data
#[must_use]
pub fn data_dir() -> PathBuf {
    match std::env::consts::OS {
        "macos" => PathBuf::from("/Users/tobiaskragholm/generated_data/parquet"),
        "linux" => PathBuf::from("/home/tkragholm/generated_data/parquet"),
        "windows" => PathBuf::from("E:\\workdata\\708245\\generated_data\\parquet"),
        _ => panic!("Unsupported operating system"),
    }
}

/// Create a path to a specific registry folder
///
/// # Arguments
/// * `registry` - The registry name (e.g., "BEF", "LPR")
///
/// # Returns
/// A path to the registry folder
#[must_use]
pub fn registry_dir(registry: &str) -> PathBuf {
    data_dir().join(registry)
}

/// Create a path to a specific file in a registry folder
///
/// # Arguments
/// * `registry` - The registry name (e.g., "BEF", "LPR")
/// * `filename` - The filename
///
/// # Returns
/// A path to the specified file
#[must_use]
pub fn registry_file(registry: &str, filename: &str) -> PathBuf {
    registry_dir(registry).join(filename)
}

/// Get all available year files from a registry directory
///
/// # Arguments
/// * `registry` - The registry name (e.g., "BEF", "LPR")
///
/// # Returns
/// A vector of paths to year files
#[must_use]
pub fn get_available_year_files(registry: &str) -> Vec<PathBuf> {
    let dir = registry_dir(registry);
    if !dir.exists() {
        return Vec::new();
    }

    std::fs::read_dir(dir)
        .ok()
        .map(|entries| {
            entries
                .filter_map(|res: std::io::Result<std::fs::DirEntry>| res.ok())
                .filter(|entry| {
                    let path = entry.path();
                    path.is_file()
                        && path.extension().is_some_and(|ext| ext == "parquet")
                        && path
                            .file_stem()
                            .is_some_and(|name| name.to_string_lossy().parse::<u32>().is_ok())
                })
                .map(|entry| entry.path())
                .collect()
        })
        .unwrap_or_default()
}