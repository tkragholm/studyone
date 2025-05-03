//! DOD (Death) Registry implementation
//!
//! This module implements the loader for the Danish Death Registry (DOD).

use arrow::array::{Array, ArrayRef, Date32Array, StringArray};
use arrow::record_batch::RecordBatch;
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use crate::error::{IdsError, Result};
use crate::schema::dod::{dod_schema, dod_standardized_schema};
use crate::schema::filter_expr::Expr;
use crate::schema::parquet_utils::{
    load_parquet_files_parallel_with_filter, read_parquet_with_filter,
};
use crate::utils::date_utils;

/// Registry loader for the Danish Death Registry (DOD)
pub struct DodRegister;

impl super::RegisterLoader for DodRegister {
    fn get_register_name(&self) -> &'static str {
        "dod"
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
                    IdsError::Data("PNR column not found in DOD data".to_string())
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
        let _schema = dod_schema();

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
            .map(|batch| standardize_dod_batch(&batch))
            .collect::<Result<Vec<_>>>()?;

        Ok(standardized_batches)
    }
}

/// Convert a DOD batch to standardized format
fn standardize_dod_batch(batch: &RecordBatch) -> Result<RecordBatch> {
    // Extract columns
    let pnr_col = batch
        .column_by_name("PNR")
        .ok_or_else(|| IdsError::Data("PNR column not found in DOD data".to_string()))?;

    let date_col = batch
        .column_by_name("DODDATO")
        .ok_or_else(|| IdsError::Data("DODDATO column not found in DOD data".to_string()))?;

    let pnr_array = pnr_col.clone();

    // Convert date column to Date32
    let date_array = if let Some(string_array) = date_col.as_any().downcast_ref::<StringArray>() {
        // Parse dates and create Date32Array
        let mut parsed_dates = Vec::with_capacity(string_array.len());
        let mut nulls = Vec::with_capacity(string_array.len());

        for i in 0..string_array.len() {
            if string_array.is_null(i) {
                parsed_dates.push(0); // Placeholder value for null
                nulls.push(i);
            } else {
                let date_str = string_array.value(i);
                if let Some(date) = date_utils::parse_danish_date(date_str) {
                    parsed_dates.push(date_utils::date_to_days_since_epoch(date));
                } else {
                    parsed_dates.push(0); // Placeholder value for null
                    nulls.push(i);
                }
            }
        }

        // Create Date32Array with nulls
        let mut values = Vec::with_capacity(parsed_dates.len());
        let mut null_buffer = Vec::with_capacity(parsed_dates.len());

        for (i, &days) in parsed_dates.iter().enumerate() {
            if nulls.contains(&i) {
                values.push(0); // Use 0 as placeholder for null values
                null_buffer.push(false);
            } else {
                values.push(days);
                null_buffer.push(true);
            }
        }

        // Create Date32Array with the values and null buffer
        let date_array = Date32Array::new(
            values.into(),
            Some(arrow::buffer::NullBuffer::new(
                arrow::buffer::BooleanBuffer::from(null_buffer),
            )),
        );
        Arc::new(date_array) as ArrayRef
    } else {
        // If it's not a StringArray (unexpected), just clone as-is and we'll handle later
        date_col.clone()
    };

    // Create standardized batch
    let standardized_batch = RecordBatch::try_new(
        Arc::new(dod_standardized_schema()),
        vec![pnr_array, date_array],
    )?;

    Ok(standardized_batch)
}
