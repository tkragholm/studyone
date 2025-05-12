//! LPR data loading module
//!
//! This module provides functions to load and process LPR data
//! from registry files.

use crate::RegistryManager;
use crate::algorithm::health::lpr_config::LprConfig;
use crate::algorithm::health::lpr_utility::{
    combine_diagnosis_collections, get_lpr_adm_files, get_lpr_diag_files, get_lpr3_diagnoser_files,
    get_lpr3_kontakter_files, match_lpr_files_by_year, match_lpr3_files_by_year,
};
use crate::algorithm::health::lpr2_processor::integrate_lpr2_components;
use crate::algorithm::health::lpr3_processor::integrate_lpr3_components;
use crate::algorithm::population::Population;
use crate::error::Result;
use crate::models::collections::ModelCollection;
use crate::models::health::diagnosis::DiagnosisCollection;

use anyhow::anyhow;
use arrow::record_batch::RecordBatch;
use std::collections::HashSet;
use std::path::PathBuf;

/// Process LPR data from both LPR2 and LPR3 sources
pub fn process_lpr_data(
    lpr2_adm: Option<&RecordBatch>,
    lpr2_diag: Option<&RecordBatch>,
    lpr2_bes: Option<&RecordBatch>,
    lpr3_kontakter: Option<&RecordBatch>,
    lpr3_diagnoser: Option<&RecordBatch>,
    config: &LprConfig,
) -> Result<DiagnosisCollection> {
    // Process LPR2 data if enabled and available
    let lpr2_collection = if config.include_lpr2 && lpr2_adm.is_some() && lpr2_diag.is_some() {
        Some(integrate_lpr2_components(
            lpr2_adm.unwrap(),
            lpr2_diag.unwrap(),
            lpr2_bes,
            config,
        )?)
    } else {
        None
    };

    // Process LPR3 data if enabled and available
    let lpr3_collection =
        if config.include_lpr3 && lpr3_kontakter.is_some() && lpr3_diagnoser.is_some() {
            Some(integrate_lpr3_components(
                lpr3_kontakter.unwrap(),
                lpr3_diagnoser.unwrap(),
                config,
            )?)
        } else {
            None
        };

    // Combine the results
    combine_diagnosis_collections(vec![lpr2_collection, lpr3_collection])
}

/// Load diagnoses from real LPR test data for all available years
///
/// This implementation uses `RegistryManager` for efficient loading and automatic type conversion
pub async fn load_diagnoses(population: &Population) -> Result<DiagnosisCollection> {
    // Check if LPR data directories exist
    let lpr2_available = check_lpr2_availability()?;
    let lpr3_available = check_lpr3_availability()?;

    // Check if at least one version is available
    if !lpr2_available && !lpr3_available {
        return Err(anyhow!(
            "LPR data directories not found. Need either LPR2 or LPR3 data."
        ));
    }

    // Create a combined diagnosis collection to store all diagnoses
    let mut combined_diagnosis_collection = DiagnosisCollection::new();
    let lpr_config = LprConfig::default();

    // Extract PNRs from the population to use as filter
    let pnrs: HashSet<String> = population
        .collection
        .get_individuals()
        .iter()
        .map(|individual| individual.pnr.clone())
        .collect();

    // Get all PNRs in the population for iterating later
    let all_pnrs: Vec<String> = population
        .collection
        .get_individuals()
        .iter()
        .map(|individual| individual.pnr.clone())
        .collect();

    // Create a registry manager for efficient loading and caching
    let manager = RegistryManager::new();

    // Process LPR2 data if available
    if lpr2_available {
        let lpr2_collection = load_lpr2_data_async(&manager, &pnrs, &all_pnrs, &lpr_config).await?;

        // Check if we have any diagnoses by looking at each PNR
        let has_diagnoses = all_pnrs
            .iter()
            .any(|pnr| !lpr2_collection.get_diagnoses(pnr).is_empty());

        if has_diagnoses {
            // Add diagnoses to combined collection
            for pnr in &all_pnrs {
                for diagnosis in lpr2_collection.get_diagnoses(pnr) {
                    combined_diagnosis_collection.add(diagnosis.as_ref().clone());
                }
            }
        }
    }

    // Process LPR3 data if available
    if lpr3_available {
        let lpr3_collection = load_lpr3_data_async(&manager, &pnrs, &all_pnrs, &lpr_config).await?;

        // Check if we have any diagnoses by looking at each PNR
        let has_diagnoses = all_pnrs
            .iter()
            .any(|pnr| !lpr3_collection.get_diagnoses(pnr).is_empty());

        if has_diagnoses {
            // Add diagnoses to combined collection
            for pnr in &all_pnrs {
                for diagnosis in lpr3_collection.get_diagnoses(pnr) {
                    combined_diagnosis_collection.add(diagnosis.as_ref().clone());
                }
            }
        }
    }

    // Count total diagnoses
    let mut total_diagnoses = 0;
    for pnr in &all_pnrs {
        total_diagnoses += combined_diagnosis_collection.get_diagnoses(pnr).len();
    }

    // Check if we loaded any diagnoses
    if total_diagnoses == 0 {
        return Err(anyhow!("No LPR data loaded from any year"));
    }

    log::info!("Total diagnoses loaded from all years: {total_diagnoses}");

    Ok(combined_diagnosis_collection)
}

/// Check if LPR2 data is available
fn check_lpr2_availability() -> Result<bool> {
    let lpr_diag_files = get_lpr_diag_files()?;
    let lpr_adm_files = get_lpr_adm_files()?;
    Ok(!lpr_diag_files.is_empty() && !lpr_adm_files.is_empty())
}

/// Check if LPR3 data is available
fn check_lpr3_availability() -> Result<bool> {
    let lpr3_kontakter_files = get_lpr3_kontakter_files()?;
    let lpr3_diagnoser_files = get_lpr3_diagnoser_files()?;
    Ok(!lpr3_kontakter_files.is_empty() && !lpr3_diagnoser_files.is_empty())
}

/// Load LPR2 data from available files asynchronously
async fn load_lpr2_data_async(
    manager: &RegistryManager,
    _pnrs: &HashSet<String>,
    all_pnrs: &[String],
    lpr_config: &LprConfig,
) -> Result<DiagnosisCollection> {
    let lpr_diag_files = get_lpr_diag_files()?;
    let lpr_adm_files = get_lpr_adm_files()?;

    // Display file count for user information
    log::info!(
        "Found {} LPR_DIAG files and {} LPR_ADM files to process",
        lpr_diag_files.len(),
        lpr_adm_files.len()
    );

    let matched_files = match_lpr_files_by_year(&lpr_diag_files, &lpr_adm_files);
    let mut combined_collection = DiagnosisCollection::new();

    // Use register manager's async loading capabilities
    for (diag_idx, (diag_file_opt, adm_file_opt)) in matched_files.iter().enumerate() {
        if diag_file_opt.is_none() || adm_file_opt.is_none() {
            continue;
        }

        let diag_file = diag_file_opt.as_ref().unwrap();
        let adm_file = adm_file_opt.as_ref().unwrap();

        // Extract year from filenames for logging
        let _year = extract_year_from_file_path(diag_file, diag_idx);

        // Register files with manager for async loading
        if !manager.has_registry("lpr_diag") {
            manager.register(
                "lpr_diag",
                diag_file.parent().unwrap_or(diag_file.as_path()),
            )?;
        }

        if !manager.has_registry("lpr_adm") {
            manager.register("lpr_adm", adm_file.parent().unwrap_or(adm_file.as_path()))?;
        }

        // Use async loading
        let diag_data = manager.load_async("lpr_diag").await?;
        let adm_data = manager.load_async("lpr_adm").await?;

        // Process the loaded data
        for (adm_batch, diag_batch) in adm_data.iter().zip(diag_data.iter()) {
            let year_collection = process_lpr_data(
                Some(adm_batch),
                Some(diag_batch),
                None, // No BES data for now
                None, // No LPR3 data
                None, // No LPR3 data
                lpr_config,
            )?;

            // Add each diagnosis to the combined collection
            for pnr in all_pnrs {
                for diagnosis in year_collection.get_diagnoses(pnr) {
                    combined_collection.add(diagnosis.as_ref().clone());
                }
            }
        }
    }

    Ok(combined_collection)
}

/// Load LPR2 data from available files using synchronous loading
#[allow(dead_code)]
fn load_lpr2_data(
    manager: &RegistryManager,
    pnrs: &HashSet<String>,
    all_pnrs: &[String],
    lpr_config: &LprConfig,
) -> Result<DiagnosisCollection> {
    let lpr_diag_files = get_lpr_diag_files()?;
    let lpr_adm_files = get_lpr_adm_files()?;

    // Display file count for user information
    log::info!(
        "Found {} LPR_DIAG files and {} LPR_ADM files to process",
        lpr_diag_files.len(),
        lpr_adm_files.len()
    );

    let matched_files = match_lpr_files_by_year(&lpr_diag_files, &lpr_adm_files);
    let mut combined_collection = DiagnosisCollection::new();

    for (diag_idx, (diag_file_opt, adm_file_opt)) in matched_files.iter().enumerate() {
        // Both files must be present
        if diag_file_opt.is_none() || adm_file_opt.is_none() {
            continue;
        }

        let diag_file = diag_file_opt.as_ref().unwrap();
        let adm_file = adm_file_opt.as_ref().unwrap();

        // Extract year from filenames for logging
        let year = extract_year_from_file_path(diag_file, diag_idx);

        log::info!(
            "Processing year {} - DIAG: {:?}, ADM: {:?}",
            year,
            diag_file.file_name().unwrap_or_default(),
            adm_file.file_name().unwrap_or_default()
        );

        // Register data sources for this year with the registry manager
        // We use unique names that include the year to avoid caching conflicts
        let diag_name = format!("lpr_diag_{year}");
        let adm_name = format!("lpr_adm_{year}");

        manager.register(&diag_name, diag_file)?;
        manager.register(&adm_name, adm_file)?;

        // Load data for this year with the PNR filter
        // This utilizes RegistryManager's caching and schema-adapting capabilities
        let filtered_data = manager.filter_by_pnr(&[&diag_name, &adm_name], pnrs)?;

        // Extract the batches for processing
        let diag_batches = filtered_data.get(&diag_name).cloned().unwrap_or_default();
        let adm_batches = filtered_data.get(&adm_name).cloned().unwrap_or_default();

        // Skip if no data
        if diag_batches.is_empty() || adm_batches.is_empty() {
            log::info!("No data for year {year}, skipping");
            continue;
        }

        // Process this year's data
        let year_diagnoses = integrate_lpr2_components(
            &adm_batches[0],  // First batch
            &diag_batches[0], // First batch
            None,             // No LPR_BES data
            lpr_config,
        )?;

        // Count diagnoses in this batch
        let mut diagnoses_count = 0;

        // Add diagnoses to combined collection by looking up each PNR
        for pnr in all_pnrs {
            let diagnoses = year_diagnoses.get_diagnoses(pnr);
            for diagnosis in diagnoses {
                combined_collection.add(diagnosis.as_ref().clone());
                diagnoses_count += 1;
            }
        }

        log::info!("Added {diagnoses_count} diagnoses from year {year}");
    }

    Ok(combined_collection)
}

/// Load LPR3 data from available files asynchronously
async fn load_lpr3_data_async(
    manager: &RegistryManager,
    _pnrs: &HashSet<String>,
    all_pnrs: &[String],
    lpr_config: &LprConfig,
) -> Result<DiagnosisCollection> {
    let lpr3_kontakter_files = get_lpr3_kontakter_files()?;
    let lpr3_diagnoser_files = get_lpr3_diagnoser_files()?;

    // Display file count for user information
    log::info!(
        "Found {} LPR3_KONTAKTER files and {} LPR3_DIAGNOSER files to process",
        lpr3_kontakter_files.len(),
        lpr3_diagnoser_files.len()
    );

    let matched_files = match_lpr3_files_by_year(&lpr3_kontakter_files, &lpr3_diagnoser_files);
    let mut combined_collection = DiagnosisCollection::new();

    // Use register manager's async loading capabilities
    for (idx, (kontakter_file_opt, diagnoser_file_opt)) in matched_files.iter().enumerate() {
        if kontakter_file_opt.is_none() || diagnoser_file_opt.is_none() {
            continue;
        }

        let kontakter_file = kontakter_file_opt.as_ref().unwrap();
        let diagnoser_file = diagnoser_file_opt.as_ref().unwrap();

        // Extract year from filenames for logging
        let _year = extract_year_from_file_path(kontakter_file, idx);

        // Register files with manager for async loading
        if !manager.has_registry("lpr3_kontakter") {
            manager.register(
                "lpr3_kontakter",
                kontakter_file.parent().unwrap_or(kontakter_file.as_path()),
            )?;
        }

        if !manager.has_registry("lpr3_diagnoser") {
            manager.register(
                "lpr3_diagnoser",
                diagnoser_file.parent().unwrap_or(diagnoser_file.as_path()),
            )?;
        }

        // Use async loading
        let kontakter_data = manager.load_async("lpr3_kontakter").await?;
        let diagnoser_data = manager.load_async("lpr3_diagnoser").await?;

        // Process the loaded data
        for (kontakter_batch, diagnoser_batch) in kontakter_data.iter().zip(diagnoser_data.iter()) {
            let year_collection = process_lpr_data(
                None, // No LPR2 data
                None, // No LPR2 data
                None, // No BES data
                Some(kontakter_batch),
                Some(diagnoser_batch),
                lpr_config,
            )?;

            // Add each diagnosis to the combined collection
            for pnr in all_pnrs {
                for diagnosis in year_collection.get_diagnoses(pnr) {
                    combined_collection.add(diagnosis.as_ref().clone());
                }
            }
        }
    }

    Ok(combined_collection)
}

/// Load LPR3 data from available files
#[allow(dead_code)]
fn load_lpr3_data(
    manager: &RegistryManager,
    pnrs: &HashSet<String>,
    all_pnrs: &[String],
    lpr_config: &LprConfig,
) -> Result<DiagnosisCollection> {
    let lpr3_kontakter_files = get_lpr3_kontakter_files()?;
    let lpr3_diagnoser_files = get_lpr3_diagnoser_files()?;

    // Display file count for user information
    log::info!(
        "Found {} LPR3_KONTAKTER files and {} LPR3_DIAGNOSER files to process",
        lpr3_kontakter_files.len(),
        lpr3_diagnoser_files.len()
    );

    let matched_files = match_lpr3_files_by_year(&lpr3_kontakter_files, &lpr3_diagnoser_files);
    let mut combined_collection = DiagnosisCollection::new();

    for (idx, (kontakt_file_opt, diagnose_file_opt)) in matched_files.iter().enumerate() {
        // Both files must be present
        if kontakt_file_opt.is_none() || diagnose_file_opt.is_none() {
            continue;
        }

        let kontakt_file = kontakt_file_opt.as_ref().unwrap();
        let diagnose_file = diagnose_file_opt.as_ref().unwrap();

        // Extract year from filenames for logging
        let year = extract_year_from_file_path(kontakt_file, idx);

        log::info!(
            "Processing year {} - KONTAKTER: {:?}, DIAGNOSER: {:?}",
            year,
            kontakt_file.file_name().unwrap_or_default(),
            diagnose_file.file_name().unwrap_or_default()
        );

        // Register data sources for this year with the registry manager
        let kontakt_name = format!("lpr3_kontakter_{year}");
        let diagnose_name = format!("lpr3_diagnoser_{year}");

        manager.register(&kontakt_name, kontakt_file)?;
        manager.register(&diagnose_name, diagnose_file)?;

        // Load data for this year with the PNR filter
        let filtered_data = manager.filter_by_pnr(&[&kontakt_name, &diagnose_name], pnrs)?;

        // Extract the batches for processing
        let kontakt_batches = filtered_data
            .get(&kontakt_name)
            .cloned()
            .unwrap_or_default();
        let diagnose_batches = filtered_data
            .get(&diagnose_name)
            .cloned()
            .unwrap_or_default();

        // Skip if no data
        if kontakt_batches.is_empty() || diagnose_batches.is_empty() {
            log::info!("No data for year {year}, skipping");
            continue;
        }

        // Process this year's data
        let year_diagnoses = integrate_lpr3_components(
            &kontakt_batches[0],  // First batch
            &diagnose_batches[0], // First batch
            lpr_config,
        )?;

        // Count diagnoses in this batch
        let mut diagnoses_count = 0;

        // Add diagnoses to combined collection by looking up each PNR
        for pnr in all_pnrs {
            let diagnoses = year_diagnoses.get_diagnoses(pnr);
            for diagnosis in diagnoses {
                combined_collection.add(diagnosis.as_ref().clone());
                diagnoses_count += 1;
            }
        }

        log::info!("Added {diagnoses_count} diagnoses from year {year}");
    }

    Ok(combined_collection)
}

/// Extract year from file path or use index as fallback
fn extract_year_from_file_path(path: &PathBuf, fallback_index: usize) -> i32 {
    let file_stem = path
        .file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_default();

    // Try parsing the file stem directly
    if let Ok(year) = file_stem.parse::<i32>() {
        return year;
    }

    // Try to extract year at the end of the filename (e.g., lpr_diag_2015)
    if let Some(last_underscore_pos) = file_stem.rfind('_') {
        let year_part = &file_stem[last_underscore_pos + 1..];
        if let Ok(year) = year_part.parse::<i32>() {
            return year;
        }
    }

    // Use index + 2000 as fallback
    2000 + fallback_index as i32
}
