//! DODSAARSAG (Death Cause) Registry implementation
//!
//! This module implements the loader for the Danish Death Cause Registry (DODSAARSAG).

use arrow::array::{Array, ArrayRef, StringArray};
use arrow::record_batch::RecordBatch;
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use crate::error::{IdsError, Result};
use crate::model::icd10::diagnosis_pattern::normalize_diagnosis_code;
use crate::model::icd10::Icd10Chapter;
use crate::schema::dodsaarsag::{dodsaarsag_schema, dodsaarsag_standardized_schema};
use crate::schema::filter_expr::Expr;
use crate::schema::parquet_utils::{
    load_parquet_files_parallel_with_filter, read_parquet_with_filter,
};

/// Registry loader for the Danish Death Cause Registry (DODSAARSAG)
pub struct DodsaarsagRegister;

impl super::RegisterLoader for DodsaarsagRegister {
    fn get_register_name(&self) -> &'static str {
        "dodsaarsag"
    }

    fn load(
        &self,
        base_path: &str,
        pnr_filter: Option<&HashSet<String>>,
    ) -> Result<Vec<RecordBatch>> {
        let path = Path::new(base_path);

        // Build PNR filter if provided
        let _predicate = pnr_filter.map(|pnrs| {
            move |batch: &RecordBatch| {
                let pnr_col = batch.column_by_name("PNR").ok_or_else(|| {
                    IdsError::Data("PNR column not found in DODSAARSAG data".to_string())
                })?;

                if let Some(pnr_array) = pnr_col.as_any().downcast_ref::<StringArray>() {
                    let mut mask = vec![false; pnr_array.len()];

                    for (i, pnr) in pnr_array.iter().enumerate() {
                        if let Some(p) = pnr {
                            mask[i] = pnrs.contains(p);
                        }
                    }

                    Ok(mask)
                } else {
                    Err(IdsError::Data(
                        "PNR column is not a StringArray".to_string(),
                    ))
                }
            }
        });

        // Load data
        let _schema = dodsaarsag_schema();

        // Create a simple equality expression for PNR filtering if needed
        let predicate_expr = if let Some(pnrs) = pnr_filter {
            let pnr_values: Vec<String> = pnrs.iter().cloned().collect();
            Expr::In("PNR".to_string(), pnr_values)
        } else {
            Expr::AlwaysTrue
        };

        let batches = if path.is_dir() {
            load_parquet_files_parallel_with_filter(path, &predicate_expr, None)?
        } else {
            read_parquet_with_filter(path, &predicate_expr, None)?
        };

        // Standardize the loaded data
        let standardized_batches = batches
            .into_iter()
            .map(|batch| standardize_dodsaarsag_batch(&batch))
            .collect::<Result<Vec<_>>>()?;

        Ok(standardized_batches)
    }
}

/// Convert a DODSAARSAG batch to standardized format
fn standardize_dodsaarsag_batch(batch: &RecordBatch) -> Result<RecordBatch> {
    // Extract columns
    let pnr_col = batch
        .column_by_name("PNR")
        .ok_or_else(|| IdsError::Data("PNR column not found in DODSAARSAG data".to_string()))?;

    let cause_col = batch.column_by_name("C_AARSAG").ok_or_else(|| {
        IdsError::Data("C_AARSAG column not found in DODSAARSAG data".to_string())
    })?;

    let condition_col = batch.column_by_name("C_TILSTAND").ok_or_else(|| {
        IdsError::Data("C_TILSTAND column not found in DODSAARSAG data".to_string())
    })?;

    let pnr_array = pnr_col.clone();
    let cause_array = cause_col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| IdsError::Data("C_AARSAG column is not a StringArray".to_string()))?;
    let condition_array = condition_col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| IdsError::Data("C_TILSTAND column is not a StringArray".to_string()))?;

    // Process and normalize causes and create chapter information
    let mut normalized_causes = Vec::with_capacity(cause_array.len());
    let mut normalized_conditions = Vec::with_capacity(condition_array.len());
    let mut chapters = Vec::with_capacity(cause_array.len());

    for i in 0..cause_array.len() {
        // Process cause code
        if cause_array.is_null(i) {
            normalized_causes.push(None);
            chapters.push(None);
        } else {
            let cause_code = cause_array.value(i);
            if let Some(normalized) = normalize_diagnosis_code(cause_code) {
                normalized_causes.push(Some(normalized.full_code.clone()));

                // Determine ICD-10 chapter
                if let Some(chapter) = Icd10Chapter::from_code(&normalized.full_code) {
                    chapters.push(Some(chapter.description().to_string()));
                } else {
                    chapters.push(None);
                }
            } else {
                normalized_causes.push(Some(cause_code.to_string()));
                chapters.push(None);
            }
        }

        // Process condition code
        if condition_array.is_null(i) {
            normalized_conditions.push(None);
        } else {
            let condition_code = condition_array.value(i);
            if let Some(normalized) = normalize_diagnosis_code(condition_code) {
                normalized_conditions.push(Some(normalized.full_code.clone()));
            } else {
                normalized_conditions.push(Some(condition_code.to_string()));
            }
        }
    }

    // Create ArrayRef objects for the new batch
    let normalized_cause_array = Arc::new(StringArray::from(normalized_causes)) as ArrayRef;
    let normalized_condition_array = Arc::new(StringArray::from(normalized_conditions)) as ArrayRef;
    let chapter_array = Arc::new(StringArray::from(chapters)) as ArrayRef;

    // Create standardized batch
    let standardized_batch = RecordBatch::try_new(
        Arc::new(dodsaarsag_standardized_schema()),
        vec![
            pnr_array,
            normalized_cause_array,
            normalized_condition_array,
            chapter_array,
        ],
    )?;

    Ok(standardized_batch)
}
