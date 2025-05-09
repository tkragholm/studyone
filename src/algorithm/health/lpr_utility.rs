//! Utility functions for LPR data processing
//!
//! This module provides common utility functions used across
//! different LPR processing components.

use crate::error::Result;
use crate::models::collections::ModelCollection;
use crate::models::diagnosis::DiagnosisCollection;
use crate::utils::test_utils::{get_available_year_files, registry_dir};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

/// Extension trait for `DiagnosisCollection`
pub trait DiagnosisCollectionExt {
    /// Get all PNRs in the collection
    fn get_all_pnrs(&self) -> Vec<String>;
}

impl DiagnosisCollectionExt for DiagnosisCollection {
    fn get_all_pnrs(&self) -> Vec<String> {
        // Implement based on DiagnosisCollection's internal structure
        // For now, we'll return an empty vector and implement this later
        // when we update the DiagnosisCollection struct
        Vec::new()
    }
}

/// Combine diagnosis collections
///
/// This function merges multiple diagnosis collections into a single collection.
pub fn combine_diagnosis_collections(
    collections: Vec<Option<DiagnosisCollection>>,
) -> Result<DiagnosisCollection> {
    let mut combined_collection = DiagnosisCollection::new();

    for collection_opt in collections {
        if let Some(collection) = collection_opt {
            for pnr in collection.get_all_pnrs() {
                for diagnosis in collection.get_diagnoses(&pnr) {
                    combined_collection.add((*diagnosis).clone());
                }
            }
        }
    }

    Ok(combined_collection)
}

/// Get available `LPR_DIAG` files
///
/// This utility function gets all available `LPR_DIAG` files from the registry directory
pub fn get_lpr_diag_files() -> Result<Vec<PathBuf>> {
    let lpr_diag_path = registry_dir("lpr_diag");
    if !lpr_diag_path.exists() {
        return Ok(Vec::new());
    }

    Ok(get_available_year_files("lpr_diag"))
}

/// Get available `LPR_ADM` files
///
/// This utility function gets all available `LPR_ADM` files from the registry directory
pub fn get_lpr_adm_files() -> Result<Vec<PathBuf>> {
    let lpr_adm_path = registry_dir("lpr_adm");
    if !lpr_adm_path.exists() {
        return Ok(Vec::new());
    }

    Ok(get_available_year_files("lpr_adm"))
}

/// Get available `LPR3_KONTAKTER` files
pub fn get_lpr3_kontakter_files() -> Result<Vec<PathBuf>> {
    let path = registry_dir("lpr3_kontakter");
    if !path.exists() {
        return Ok(Vec::new());
    }

    Ok(get_available_year_files("lpr3_kontakter"))
}

/// Get available `LPR3_DIAGNOSER` files
pub fn get_lpr3_diagnoser_files() -> Result<Vec<PathBuf>> {
    let path = registry_dir("lpr3_diagnoser");
    if !path.exists() {
        return Ok(Vec::new());
    }

    Ok(get_available_year_files("lpr3_diagnoser"))
}

/// Extract year from a file path
///
/// Attempts to extract the year from a file name, typically in formats like
/// "2010.parquet", "`lpr_diag_2015.parquet`", etc.
#[must_use] pub fn extract_year_from_file_path(path: &PathBuf) -> Option<i32> {
    let file_stem = path.file_stem()?.to_string_lossy();

    // Try to parse the entire file stem as a year
    if let Ok(year) = file_stem.parse::<i32>() {
        return Some(year);
    }

    // Try to extract year at the end of the filename (e.g., lpr_diag_2015)
    if let Some(last_underscore_pos) = file_stem.rfind('_') {
        let year_part = &file_stem[last_underscore_pos + 1..];
        if let Ok(year) = year_part.parse::<i32>() {
            return Some(year);
        }
    }

    None
}

/// Match corresponding LPR files by year
///
/// This function takes vectors of LPR file paths and matches them by year
/// to ensure data is processed with the correct complementary files.
#[must_use] pub fn match_lpr_files_by_year(
    diag_files: &[PathBuf],
    adm_files: &[PathBuf],
) -> Vec<(Option<PathBuf>, Option<PathBuf>)> {
    let mut matched_files = Vec::new();
    let mut diag_by_year: HashMap<i32, PathBuf> = HashMap::new();
    let mut adm_by_year: HashMap<i32, PathBuf> = HashMap::new();

    // Index files by year
    for diag_file in diag_files {
        if let Some(year) = extract_year_from_file_path(diag_file) {
            diag_by_year.insert(year, diag_file.clone());
        }
    }

    for adm_file in adm_files {
        if let Some(year) = extract_year_from_file_path(adm_file) {
            adm_by_year.insert(year, adm_file.clone());
        }
    }

    // Get all years from both collections
    let mut all_years: Vec<i32> = diag_by_year
        .keys()
        .chain(adm_by_year.keys())
        .copied()
        .collect::<HashSet<i32>>()
        .into_iter()
        .collect();

    // Sort years chronologically
    all_years.sort_unstable();

    // Match files by year
    for year in all_years {
        let diag_file = diag_by_year.get(&year).cloned();
        let adm_file = adm_by_year.get(&year).cloned();
        matched_files.push((diag_file, adm_file));
    }

    matched_files
}

/// Match corresponding LPR3 files by year
#[must_use] pub fn match_lpr3_files_by_year(
    kontakter_files: &[PathBuf],
    diagnoser_files: &[PathBuf],
) -> Vec<(Option<PathBuf>, Option<PathBuf>)> {
    let mut matched_files = Vec::new();
    let mut kontakter_by_year: HashMap<i32, PathBuf> = HashMap::new();
    let mut diagnoser_by_year: HashMap<i32, PathBuf> = HashMap::new();

    // Index files by year
    for kontakt_file in kontakter_files {
        if let Some(year) = extract_year_from_file_path(kontakt_file) {
            kontakter_by_year.insert(year, kontakt_file.clone());
        }
    }

    for diagnose_file in diagnoser_files {
        if let Some(year) = extract_year_from_file_path(diagnose_file) {
            diagnoser_by_year.insert(year, diagnose_file.clone());
        }
    }

    // Get all years from both collections
    let mut all_years: Vec<i32> = kontakter_by_year
        .keys()
        .chain(diagnoser_by_year.keys())
        .copied()
        .collect::<HashSet<i32>>()
        .into_iter()
        .collect();

    // Sort years chronologically
    all_years.sort_unstable();

    // Match files by year
    for year in all_years {
        let kontakt_file = kontakter_by_year.get(&year).cloned();
        let diagnose_file = diagnoser_by_year.get(&year).cloned();
        matched_files.push((kontakt_file, diagnose_file));
    }

    matched_files
}

/// Filter diagnoses by a list of ICD-10 codes
///
/// This function filters a `DiagnosisCollection` to only include diagnoses
/// that match any of the provided ICD-10 codes (prefix match).
#[must_use] pub fn filter_diagnoses_by_icd10(
    collection: &DiagnosisCollection,
    icd10_codes: &[&str],
) -> DiagnosisCollection {
    let mut filtered_collection = DiagnosisCollection::new();

    // Normalize the ICD-10 codes for comparison
    let normalized_codes: Vec<String> = icd10_codes
        .iter()
        .map(|code| code.to_uppercase().replace(' ', ""))
        .collect();

    // Iterate through all PNRs in the collection
    for pnr in collection.get_all_pnrs() {
        for diagnosis in collection.get_diagnoses(&pnr) {
            let diag_code = diagnosis.diagnosis_code.to_uppercase();

            // Check if the diagnosis code starts with any of the target codes
            if normalized_codes
                .iter()
                .any(|code| diag_code.starts_with(code))
            {
                filtered_collection.add((*diagnosis).clone());
            }
        }
    }

    filtered_collection
}

/// Get the standard mapping for LPR columns
///
/// Returns the standard mappings between registry column names and
/// standardized column names for LPR2 and LPR3.
#[must_use] pub fn get_standard_lpr_mappings() -> (HashMap<String, String>, HashMap<String, String>) {
    let mut lpr2_mapping = HashMap::new();
    lpr2_mapping.insert("PNR".to_string(), "patient_id".to_string());
    lpr2_mapping.insert("C_ADIAG".to_string(), "primary_diagnosis".to_string());
    lpr2_mapping.insert("C_DIAGTYPE".to_string(), "diagnosis_type".to_string());
    lpr2_mapping.insert("D_INDDTO".to_string(), "admission_date".to_string());
    lpr2_mapping.insert("D_UDDTO".to_string(), "discharge_date".to_string());

    let mut lpr3_mapping = HashMap::new();
    lpr3_mapping.insert("cpr".to_string(), "patient_id".to_string());
    lpr3_mapping.insert("diagnosekode".to_string(), "primary_diagnosis".to_string());
    lpr3_mapping.insert("diagnose_type".to_string(), "diagnosis_type".to_string());
    lpr3_mapping.insert("starttidspunkt".to_string(), "admission_date".to_string());
    lpr3_mapping.insert("sluttidspunkt".to_string(), "discharge_date".to_string());

    (lpr2_mapping, lpr3_mapping)
}
