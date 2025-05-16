//! Test fixtures and data paths
//!
//! This module provides utilities for accessing test data fixtures.

use std::path::PathBuf;

/// Base path for test data files
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
#[must_use]
pub fn registry_dir(registry: &str) -> PathBuf {
    data_dir().join(registry)
}

/// Create a path to a specific file in a registry folder
#[must_use]
pub fn registry_file(registry: &str, filename: &str) -> PathBuf {
    registry_dir(registry).join(filename)
}