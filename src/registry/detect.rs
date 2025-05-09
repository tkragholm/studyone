//! Registry type detection utilities
//!
//! This module provides utilities for detecting registry types from data schemas.
//! It centralizes the registry detection logic to ensure consistency across the codebase.

use arrow::record_batch::RecordBatch;
use log::debug;

/// Registry type identifier constants
pub enum RegistryType {
    /// BEF - Population registry
    BEF,
    /// IND - Income registry
    IND,
    /// LPR - Patient registry
    LPR,
    /// MFR - Medical birth registry
    MFR,
    /// VNDS - Migration registry
    VNDS,
    /// DOD - Death registry
    DOD,
    /// AKM - Employment registry
    AKM,
    /// UDDF - Education registry
    UDDF,
    /// Unknown registry type
    Unknown,
}

impl RegistryType {
    /// Convert `RegistryType` to static string
    #[must_use] pub fn as_str(&self) -> &'static str {
        match self {
            RegistryType::BEF => "BEF",
            RegistryType::IND => "IND",
            RegistryType::LPR => "LPR",
            RegistryType::MFR => "MFR",
            RegistryType::VNDS => "VNDS",
            RegistryType::DOD => "DOD",
            RegistryType::AKM => "AKM",
            RegistryType::UDDF => "UDDF",
            RegistryType::Unknown => "UNKNOWN",
        }
    }
}

impl From<&str> for RegistryType {
    fn from(s: &str) -> Self {
        match s.trim().to_uppercase().as_str() {
            "BEF" => RegistryType::BEF,
            "IND" => RegistryType::IND,
            "LPR" => RegistryType::LPR,
            "MFR" => RegistryType::MFR,
            "VNDS" => RegistryType::VNDS,
            "DOD" => RegistryType::DOD,
            "AKM" => RegistryType::AKM,
            "UDDF" => RegistryType::UDDF,
            _ => RegistryType::Unknown,
        }
    }
}

/// Detect registry type from batch schema
///
/// This function examines the schema of a `RecordBatch` and detects which
/// registry it belongs to based on characteristic field names.
///
/// # Arguments
///
/// * `batch` - The `RecordBatch` to examine
///
/// # Returns
///
/// The detected registry type as a `RegistryType` enum
#[must_use] pub fn detect_registry_type(batch: &RecordBatch) -> RegistryType {
    let registry_type = if batch.schema().field_with_name("RECNUM").is_ok() {
        RegistryType::LPR
    } else if batch.schema().field_with_name("PERINDKIALT").is_ok() {
        RegistryType::IND
    } else if batch.schema().field_with_name("BARSELNR").is_ok() {
        RegistryType::MFR
    } else if batch.schema().field_with_name("VEJ_KODE").is_ok() {
        RegistryType::VNDS
    } else if batch.schema().field_with_name("DODDATO").is_ok() {
        RegistryType::DOD
    } else if batch.schema().field_with_name("HELTID").is_ok() {
        RegistryType::AKM
    } else if batch.schema().field_with_name("UDD_H").is_ok() {
        RegistryType::UDDF
    } else {
        // Default to BEF registry format
        RegistryType::BEF
    };

    debug!("Detected registry type: {}", registry_type.as_str());
    registry_type
}

/// Utility function for legacy code compatibility - returns string instead of enum
///
/// This function is provided for backward compatibility with existing code
/// that expects a string return type.
///
/// # Arguments
///
/// * `batch` - The `RecordBatch` to examine
///
/// # Returns
///
/// The detected registry type as a static string
#[must_use] pub fn detect_registry_type_as_str(batch: &RecordBatch) -> &'static str {
    detect_registry_type(batch).as_str()
}