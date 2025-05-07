//! LPR Integration for Health Data Processing
//!
//! This module implements functions to harmonize and combine data from
//! the Danish National Patient Registry (LPR) across different versions (LPR2 and LPR3).

use crate::error::{ParquetReaderError, Result};
use crate::models::diagnosis::{Diagnosis, DiagnosisCollection, DiagnosisType};
use crate::RegistryManager;
use crate::utils::test_utils::{get_available_year_files, registry_dir};
use crate::algorithm::population::Population;
use chrono::NaiveDate;

use arrow::array::{Array, Date32Array, StringArray};
use arrow::record_batch::RecordBatch;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

/// Configuration for LPR data processing
#[derive(Debug, Clone)]
pub struct LprConfig {
    /// Whether to include LPR2 data
    pub include_lpr2: bool,
    /// Whether to include LPR3 data
    pub include_lpr3: bool,
    /// Start date for filtering (inclusive)
    pub start_date: Option<NaiveDate>,
    /// End date for filtering (inclusive)
    pub end_date: Option<NaiveDate>,
    /// Columns to map from LPR2 (key=source column, value=target column)
    pub lpr2_column_mapping: HashMap<String, String>,
    /// Columns to map from LPR3 (key=source column, value=target column)
    pub lpr3_column_mapping: HashMap<String, String>,
}

impl Default for LprConfig {
    fn default() -> Self {
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

        Self {
            include_lpr2: true,
            include_lpr3: true,
            start_date: None,
            end_date: None,
            lpr2_column_mapping: lpr2_mapping,
            lpr3_column_mapping: lpr3_mapping,
        }
    }
}

/// Integrate LPR2 components (`LPR_ADM`, `LPR_DIAG`, and optionally `LPR_BES`)
pub fn integrate_lpr2_components(
    lpr_adm: &RecordBatch,
    lpr_diag: &RecordBatch,
    lpr_bes: Option<&RecordBatch>,
    config: &LprConfig,
) -> Result<DiagnosisCollection> {
    use crate::models::adapters::adapter_utils;
    use crate::utils::arrow_utils;
    use arrow::datatypes::DataType;

    let mut diagnosis_collection = DiagnosisCollection::new();

    // Extract required columns from LPR_ADM using adapter_utils for better type handling
    // This automatically handles type conversions using the schema adaptation system
    let pnr_col_opt = adapter_utils::get_column(
        lpr_adm, 
        "PNR", 
        &DataType::Utf8, 
        true
    )?;
    
    let pnr_array = pnr_col_opt
        .as_ref()
        .and_then(|col| col.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| ParquetReaderError::InvalidDataType {
            column: "PNR".to_string(),
            expected: "StringArray".to_string(),
        })?;

    let primary_diag_col_opt = adapter_utils::get_column(
        lpr_adm, 
        "C_ADIAG", 
        &DataType::Utf8, 
        false
    )?;
    
    let primary_diag_array = match &primary_diag_col_opt {
        Some(col) => col.as_any().downcast_ref::<StringArray>().ok_or_else(|| 
            ParquetReaderError::InvalidDataType {
                column: "C_ADIAG".to_string(),
                expected: "StringArray".to_string(),
            })?,
        None => {
            log::warn!("C_ADIAG column not found in LPR_ADM data");
            // Create an empty array as fallback
            &StringArray::from(Vec::<Option<&str>>::new())
        }
    };

    // For date columns, use adapter_utils which will handle conversion if needed
    let date_col_opt = adapter_utils::get_column(
        lpr_adm, 
        "D_INDDTO", 
        &DataType::Date32, 
        false
    )?;

    // Extract required columns from LPR_DIAG using adapter_utils
    let diag_recnum_col_opt = adapter_utils::get_column(
        lpr_diag, 
        "RECNUM", 
        &DataType::Utf8, 
        true
    )?;
    
    let diag_recnum_array = diag_recnum_col_opt
        .as_ref()
        .and_then(|col| col.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| ParquetReaderError::InvalidDataType {
            column: "RECNUM".to_string(),
            expected: "StringArray".to_string(),
        })?;

    let diag_col_opt = adapter_utils::get_column(
        lpr_diag, 
        "C_DIAG", 
        &DataType::Utf8, 
        true
    )?;
    
    let diag_array = diag_col_opt
        .as_ref()
        .and_then(|col| col.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| ParquetReaderError::InvalidDataType {
            column: "C_DIAG".to_string(),
            expected: "StringArray".to_string(),
        })?;

    let diag_type_col_opt = adapter_utils::get_column(
        lpr_diag, 
        "C_DIAGTYPE", 
        &DataType::Utf8, 
        false
    )?;
    
    let diag_type_array = match &diag_type_col_opt {
        Some(col) => col.as_any().downcast_ref::<StringArray>().ok_or_else(|| 
            ParquetReaderError::InvalidDataType {
                column: "C_DIAGTYPE".to_string(),
                expected: "StringArray".to_string(),
            })?,
        None => {
            log::warn!("C_DIAGTYPE column not found in LPR_DIAG data");
            // Create an empty array as fallback
            &StringArray::from(Vec::<Option<&str>>::new())
        }
    };

    // Get record number column from LPR_ADM to link with LPR_DIAG
    let adm_recnum_col_opt = adapter_utils::get_column(
        lpr_adm, 
        "RECNUM", 
        &DataType::Utf8, 
        true
    )?;
    
    let adm_recnum_array = adm_recnum_col_opt
        .as_ref()
        .and_then(|col| col.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| ParquetReaderError::InvalidDataType {
            column: "RECNUM".to_string(),
            expected: "StringArray".to_string(),
        })?;

    // Create a map of diagnoses by record number
    let mut diagnoses_by_recnum: HashMap<String, Vec<(String, DiagnosisType)>> = HashMap::new();

    for i in 0..diag_recnum_array.len() {
        if diag_recnum_array.is_null(i) || diag_array.is_null(i) {
            continue;
        }

        let recnum = diag_recnum_array.value(i).to_string();
        let diagnosis = diag_array.value(i).to_string();
        let diag_type = if i >= diag_type_array.len() || diag_type_array.is_null(i) {
            DiagnosisType::Other
        } else {
            DiagnosisType::from(diag_type_array.value(i))
        };

        diagnoses_by_recnum
            .entry(recnum)
            .or_default()
            .push((diagnosis, diag_type));
    }

    // Process each admission record
    for i in 0..lpr_adm.num_rows() {
        if pnr_array.is_null(i) {
            continue;
        }

        let pnr = pnr_array.value(i).to_string();

        // Extract date using arrow_utils which handles null values and type conversion better
        let diagnosis_date = match &date_col_opt {
            Some(date_col) => {
                arrow_utils::arrow_array_to_date(date_col, i)
            },
            None => None
        };

        // Skip if outside date range
        if let Some(start_date) = config.start_date {
            if let Some(record_date) = diagnosis_date {
                if record_date < start_date {
                    continue;
                }
            }
        }

        if let Some(end_date) = config.end_date {
            if let Some(record_date) = diagnosis_date {
                if record_date > end_date {
                    continue;
                }
            }
        }

        // Add primary diagnosis if available
        if i < primary_diag_array.len() && !primary_diag_array.is_null(i) {
            let primary_diagnosis = primary_diag_array.value(i).to_string();
            
            let diagnosis = Diagnosis::new(
                pnr.clone(),
                primary_diagnosis,
                DiagnosisType::Primary,
                diagnosis_date,
            );

            diagnosis_collection.add_diagnosis(diagnosis);
        }

        // Add secondary diagnoses if available
        if !adm_recnum_array.is_null(i) {
            let recnum = adm_recnum_array.value(i).to_string();

            if let Some(diagnoses) = diagnoses_by_recnum.get(&recnum) {
                for (diagnosis_code, diagnosis_type) in diagnoses {
                    // Skip if it's already the primary diagnosis
                    if i < primary_diag_array.len() && !primary_diag_array.is_null(i)
                        && primary_diag_array.value(i) == diagnosis_code
                    {
                        continue;
                    }

                    let diagnosis = Diagnosis::new(
                        pnr.clone(),
                        diagnosis_code.clone(),
                        *diagnosis_type,
                        diagnosis_date,
                    );

                    diagnosis_collection.add_diagnosis(diagnosis);
                }
            }
        }
    }

    // Process LPR_BES (procedure) data if available
    if let Some(_lpr_bes) = lpr_bes {
        // Implementation for procedure data would go here
        // This would typically add procedure counts or other procedure-related information
        log::info!("LPR_BES data provided but not currently used in diagnosis processing");
    }

    Ok(diagnosis_collection)
}

/// Integrate LPR3 components (`LPR3_KONTAKTER` and `LPR3_DIAGNOSER`)
pub fn integrate_lpr3_components(
    lpr3_kontakter: &RecordBatch,
    lpr3_diagnoser: &RecordBatch,
    config: &LprConfig,
) -> Result<DiagnosisCollection> {
    let mut diagnosis_collection = DiagnosisCollection::new();

    // Extract required columns from LPR3_KONTAKTER
    let kontakt_id_col = lpr3_kontakter
        .column_by_name("kontakt_id")
        .ok_or_else(|| ParquetReaderError::column_not_found("kontakt_id"))?;
    let kontakt_id_array = kontakt_id_col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| ParquetReaderError::InvalidDataType {
            column: "kontakt_id".to_string(),
            expected: "StringArray".to_string(),
        })?;

    let pnr_col = lpr3_kontakter
        .column_by_name("cpr")
        .ok_or_else(|| ParquetReaderError::column_not_found("cpr"))?;
    let pnr_array = pnr_col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| ParquetReaderError::InvalidDataType {
            column: "cpr".to_string(),
            expected: "StringArray".to_string(),
        })?;

    let date_col = lpr3_kontakter
        .column_by_name("starttidspunkt")
        .ok_or_else(|| ParquetReaderError::column_not_found("starttidspunkt"))?;
    let date_array = date_col
        .as_any()
        .downcast_ref::<Date32Array>()
        .ok_or_else(|| ParquetReaderError::InvalidDataType {
            column: "starttidspunkt".to_string(),
            expected: "Date32Array".to_string(),
        })?;

    // Extract required columns from LPR3_DIAGNOSER
    let diag_kontakt_id_col = lpr3_diagnoser
        .column_by_name("kontakt_id")
        .ok_or_else(|| ParquetReaderError::column_not_found("kontakt_id"))?;
    let diag_kontakt_id_array = diag_kontakt_id_col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| ParquetReaderError::InvalidDataType {
            column: "kontakt_id".to_string(),
            expected: "StringArray".to_string(),
        })?;

    let diag_col = lpr3_diagnoser
        .column_by_name("diagnosekode")
        .ok_or_else(|| ParquetReaderError::column_not_found("diagnosekode"))?;
    let diag_array = diag_col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| ParquetReaderError::InvalidDataType {
            column: "diagnosekode".to_string(),
            expected: "StringArray".to_string(),
        })?;

    let diag_type_col = lpr3_diagnoser
        .column_by_name("diagnose_type")
        .ok_or_else(|| ParquetReaderError::column_not_found("diagnose_type"))?;
    let diag_type_array = diag_type_col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| ParquetReaderError::InvalidDataType {
            column: "diagnose_type".to_string(),
            expected: "StringArray".to_string(),
        })?;

    // Create a map of diagnoses by contact ID
    let mut diagnoses_by_kontakt_id: HashMap<String, Vec<(String, DiagnosisType)>> = HashMap::new();

    for i in 0..diag_kontakt_id_array.len() {
        if diag_kontakt_id_array.is_null(i) || diag_array.is_null(i) {
            continue;
        }

        let kontakt_id = diag_kontakt_id_array.value(i).to_string();
        let diagnosis = diag_array.value(i).to_string();
        let diag_type = if diag_type_array.is_null(i) {
            DiagnosisType::Other
        } else {
            let type_str = diag_type_array.value(i);
            // In LPR3, 'A' is the action diagnosis (primary)
            if type_str == "A" {
                DiagnosisType::Primary
            } else {
                DiagnosisType::from(type_str)
            }
        };

        diagnoses_by_kontakt_id
            .entry(kontakt_id)
            .or_default()
            .push((diagnosis, diag_type));
    }

    // Create index from kontakt_id to row for faster lookup
    let mut kontakt_id_to_row: HashMap<String, usize> = HashMap::new();
    for i in 0..kontakt_id_array.len() {
        if !kontakt_id_array.is_null(i) {
            kontakt_id_to_row.insert(kontakt_id_array.value(i).to_string(), i);
        }
    }

    // Process each diagnosis
    for (kontakt_id, diagnoses) in &diagnoses_by_kontakt_id {
        if let Some(&row_idx) = kontakt_id_to_row.get(kontakt_id) {
            if pnr_array.is_null(row_idx) {
                continue;
            }

            let pnr = pnr_array.value(row_idx).to_string();

            // Skip if outside date range
            if let Some(start_date) = config.start_date {
                if !date_array.is_null(row_idx) {
                    let record_date = arrow_date_to_naive_date(date_array.value(row_idx));
                    if record_date < start_date {
                        continue;
                    }
                }
            }

            if let Some(end_date) = config.end_date {
                if !date_array.is_null(row_idx) {
                    let record_date = arrow_date_to_naive_date(date_array.value(row_idx));
                    if record_date > end_date {
                        continue;
                    }
                }
            }

            let diagnosis_date = if date_array.is_null(row_idx) {
                None
            } else {
                Some(arrow_date_to_naive_date(date_array.value(row_idx)))
            };

            // Add all diagnoses for this contact
            for (diagnosis_code, diagnosis_type) in diagnoses {
                let diagnosis = Diagnosis::new(
                    pnr.clone(),
                    diagnosis_code.clone(),
                    *diagnosis_type,
                    diagnosis_date,
                );

                diagnosis_collection.add_diagnosis(diagnosis);
            }
        }
    }

    Ok(diagnosis_collection)
}

/// Combine diagnosis collections from LPR2 and LPR3
pub fn combine_diagnosis_collections(
    lpr2_collection: Option<DiagnosisCollection>,
    lpr3_collection: Option<DiagnosisCollection>,
) -> Result<DiagnosisCollection> {
    let mut combined_collection = DiagnosisCollection::new();

    // Add diagnoses from LPR2 if available
    if let Some(lpr2) = lpr2_collection {
        for pnr in lpr2.get_all_pnrs() {
            for diagnosis in lpr2.get_diagnoses(&pnr) {
                combined_collection.add_diagnosis((*diagnosis).clone());
            }
        }
    }

    // Add diagnoses from LPR3 if available
    if let Some(lpr3) = lpr3_collection {
        for pnr in lpr3.get_all_pnrs() {
            for diagnosis in lpr3.get_diagnoses(&pnr) {
                combined_collection.add_diagnosis((*diagnosis).clone());
            }
        }
    }

    Ok(combined_collection)
}

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
    combine_diagnosis_collections(lpr2_collection, lpr3_collection)
}

/// Convert Arrow Date32 value to `NaiveDate`
fn arrow_date_to_naive_date(days_since_epoch: i32) -> NaiveDate {
    // Using a non-const approach for the date calculation
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    epoch.checked_add_days(chrono::Days::new(days_since_epoch as u64))
        .unwrap_or(epoch)
}

/// Extension trait for `DiagnosisCollection`
pub trait DiagnosisCollectionExt {
    /// Get all PNRs in the collection
    fn get_all_pnrs(&self) -> Vec<String>;
}

impl DiagnosisCollectionExt for DiagnosisCollection {
    fn get_all_pnrs(&self) -> Vec<String> {
        // We need to add a method to the DiagnosisCollection to get all PNRs
        // For now, we'll return an empty vector and implement this later
        // when we update the DiagnosisCollection struct
        Vec::new()
    }
}

/// Get available LPR_DIAG files
///
/// This utility function gets all available LPR_DIAG files from the registry directory
pub fn get_lpr_diag_files() -> Result<Vec<PathBuf>> {
    let lpr_diag_path = registry_dir("lpr_diag");
    if !lpr_diag_path.exists() {
        return Ok(Vec::new());
    }

    Ok(get_available_year_files("lpr_diag"))
}

/// Get available LPR_ADM files
///
/// This utility function gets all available LPR_ADM files from the registry directory
pub fn get_lpr_adm_files() -> Result<Vec<PathBuf>> {
    let lpr_adm_path = registry_dir("lpr_adm");
    if !lpr_adm_path.exists() {
        return Ok(Vec::new());
    }

    Ok(get_available_year_files("lpr_adm"))
}

/// Load diagnoses from real LPR test data for all available years
///
/// This implementation uses RegistryManager for efficient loading and automatic type conversion
pub fn load_real_diagnoses(population: &Population) -> Result<DiagnosisCollection> {
    // Check if LPR data directories exist
    let lpr_diag_path = registry_dir("lpr_diag");
    let lpr_adm_path = registry_dir("lpr_adm");

    // Check if at least one of the LPR directories exists
    if !lpr_diag_path.exists() && !lpr_adm_path.exists() {
        return Err(anyhow::anyhow!(
            "LPR data directories not found. Need either lpr_diag or lpr_adm."
        )
        .into());
    }

    // Get all available LPR files
    let lpr_diag_files = get_lpr_diag_files()?;
    let lpr_adm_files = get_lpr_adm_files()?;

    // Log what we found to help debug
    log::info!("Found {} LPR_DIAG files", lpr_diag_files.len());
    log::info!("Found {} LPR_ADM files", lpr_adm_files.len());

    if lpr_diag_files.is_empty() || lpr_adm_files.is_empty() {
        log::info!("Not enough LPR files found. Will skip diagnosis processing.");
        // Return empty diagnosis collection instead of error
        return Ok(DiagnosisCollection::new());
    }

    // Extract PNRs from the population to use as filter
    let pnrs: HashSet<String> = population
        .collection
        .get_individuals()
        .iter()
        .map(|individual| individual.pnr.clone())
        .collect();

    // Create a combined diagnosis collection to store all diagnoses
    let mut combined_diagnosis_collection = DiagnosisCollection::new();
    let lpr_config = LprConfig::default();

    // Display file count for user information
    log::info!(
        "Found {} LPR_DIAG files and {} LPR_ADM files to process",
        lpr_diag_files.len(),
        lpr_adm_files.len()
    );

    // Get all PNRs in the population for iterating later
    let all_pnrs: Vec<String> = population
        .collection
        .get_individuals()
        .iter()
        .map(|individual| individual.pnr.clone())
        .collect();

    // Create a registry manager for efficient loading and caching
    let manager = RegistryManager::new();

    // For each year, process the data using RegistryManager
    for (diag_idx, diag_file) in lpr_diag_files.iter().enumerate() {
        // Try to find matching ADM file by getting the same index
        if diag_idx >= lpr_adm_files.len() {
            log::info!("No matching LPR_ADM file for {:?}, skipping", diag_file);
            continue;
        }

        let adm_file = &lpr_adm_files[diag_idx];

        // Extract year from filenames for logging
        let diag_year = diag_file
            .file_stem()
            .and_then(|name| name.to_string_lossy().parse::<u32>().ok())
            .unwrap_or(0);

        log::info!(
            "Processing year {} - DIAG: {:?}, ADM: {:?}",
            diag_year,
            diag_file.file_name().unwrap_or_default(),
            adm_file.file_name().unwrap_or_default()
        );

        // Register data sources for this year with the registry manager
        // We use unique names that include the year to avoid caching conflicts
        let diag_name = format!("lpr_diag_{}", diag_year);
        let adm_name = format!("lpr_adm_{}", diag_year);

        manager.register(&diag_name, diag_file)?;
        manager.register(&adm_name, adm_file)?;

        // Load data for this year with the PNR filter
        // This utilizes RegistryManager's caching and schema-adapting capabilities
        let filtered_data = manager.filter_by_pnr(&[&diag_name, &adm_name], &pnrs)?;

        // Extract the batches for processing
        let diag_batches = filtered_data.get(&diag_name).cloned().unwrap_or_default();
        let adm_batches = filtered_data.get(&adm_name).cloned().unwrap_or_default();

        // Skip if no data
        if diag_batches.is_empty() || adm_batches.is_empty() {
            log::info!("No data for year {}, skipping", diag_year);
            continue;
        }

        // Process this year's data
        let year_diagnoses = integrate_lpr2_components(
            &adm_batches[0],  // First batch
            &diag_batches[0], // First batch
            None,             // No LPR_BES data
            &lpr_config,
        )?;

        // Count diagnoses in this batch
        let mut diagnoses_count = 0;

        // Add diagnoses to combined collection by looking up each PNR
        for pnr in &all_pnrs {
            let diagnoses = year_diagnoses.get_diagnoses(pnr);
            for diagnosis in diagnoses {
                combined_diagnosis_collection.add_diagnosis(diagnosis.as_ref().clone());
                diagnoses_count += 1;
            }
        }

        log::info!(
            "Added {} diagnoses from year {}",
            diagnoses_count, diag_year
        );
    }

    // Count total diagnoses by iterating through all PNRs
    let mut total_diagnoses = 0;
    for pnr in &all_pnrs {
        total_diagnoses += combined_diagnosis_collection.get_diagnoses(pnr).len();
    }

    // Check if we loaded any diagnoses
    if total_diagnoses == 0 {
        return Err(anyhow::anyhow!("No LPR data loaded from any year").into());
    }

    log::info!("Total diagnoses loaded from all years: {}", total_diagnoses);

    Ok(combined_diagnosis_collection)
}
