//! Data transformation utilities for registry data
//!
//! This module provides functionality for filtering and transforming Arrow record batches.

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float64Array, Int32Array, StringArray,
};
use arrow::compute::filter as filter_batch;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{Datelike, NaiveDate};
use std::collections::HashMap;
use std::sync::Arc;

use crate::error::{IdsError, Result};

/// Transform multiple record batches using a provided transformation function
pub fn transform_records(
    batches: &[RecordBatch],
    transformation: &dyn Fn(&RecordBatch) -> Result<RecordBatch>,
) -> Result<Vec<RecordBatch>> {
    let mut result = Vec::with_capacity(batches.len());

    for batch in batches {
        let transformed = transformation(batch)?;
        // Only add if the transformed batch has rows
        if transformed.num_rows() > 0 {
            result.push(transformed);
        }
    }

    Ok(result)
}

/// Filter record batches by date range
pub fn filter_by_date_range(
    batch: &RecordBatch,
    date_column: &str,
    start_date: Option<NaiveDate>,
    end_date: Option<NaiveDate>,
) -> Result<RecordBatch> {
    // Find the date column
    let date_idx = batch
        .schema()
        .index_of(date_column)
        .map_err(|e| IdsError::Validation(format!("Date column '{date_column}' not found: {e}")))?;

    let date_array = batch.column(date_idx);
    let date_array = date_array
        .as_any()
        .downcast_ref::<Date32Array>()
        .ok_or_else(|| {
            IdsError::Validation(format!("Column '{date_column}' is not a Date32 array"))
        })?;

    // Create a boolean mask for date filtering
    let mut mask_values = Vec::with_capacity(batch.num_rows());

    for i in 0..date_array.len() {
        if date_array.is_null(i) {
            mask_values.push(false);
            continue;
        }

        let days = date_array.value(i);
        let date = NaiveDate::from_num_days_from_ce_opt(days + 719163).unwrap_or_default();

        let in_range = (start_date.is_none() || date >= start_date.unwrap())
            && (end_date.is_none() || date <= end_date.unwrap());

        mask_values.push(in_range);
    }

    let mask = BooleanArray::from(mask_values);

    // Apply the filter to all columns
    let filtered_columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|col| filter_batch(col, &mask))
        .collect::<arrow::error::Result<_>>()
        .map_err(|e| IdsError::Data(format!("Failed to filter batch by date range: {e}")))?;

    // Create a new record batch with filtered data
    RecordBatch::try_new(batch.schema(), filtered_columns)
        .map_err(|e| IdsError::Data(format!("Failed to create filtered batch: {e}")))
}

/// Extract year from date column and add it as a new column
pub fn add_year_column(batch: &RecordBatch, date_column: &str) -> Result<RecordBatch> {
    // Find the date column
    let date_idx = batch
        .schema()
        .index_of(date_column)
        .map_err(|e| IdsError::Validation(format!("Date column '{date_column}' not found: {e}")))?;

    let date_array = batch.column(date_idx);
    let date_array = date_array
        .as_any()
        .downcast_ref::<Date32Array>()
        .ok_or_else(|| {
            IdsError::Validation(format!("Column '{date_column}' is not a Date32 array"))
        })?;

    // Create a new Int32Array with year values
    let mut year_values = Vec::with_capacity(batch.num_rows());

    for i in 0..date_array.len() {
        if date_array.is_null(i) {
            year_values.push(None);
        } else {
            let days = date_array.value(i);
            let date = NaiveDate::from_num_days_from_ce_opt(days + 719163).unwrap_or_default();
            year_values.push(Some(date.year()));
        }
    }

    // Create the Int32Array for the years
    let year_array = Int32Array::from(year_values);

    // Create a new field for the year column
    let year_field = Field::new("year", DataType::Int32, true);

    // Create a new schema by adding the year field
    let schema = batch.schema();
    let fields = schema.fields();
    let mut field_vec = fields.to_vec();
    field_vec.push(Arc::new(year_field));
    let new_schema = Arc::new(Schema::new(field_vec));

    // Create a new record batch with all the original columns plus the year column
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns.push(Arc::new(year_array));

    // Create a new record batch with the new schema and columns
    RecordBatch::try_new(new_schema, columns).map_err(|e| {
        IdsError::Data(format!(
            "Failed to create record batch with year column: {e}"
        ))
    })
}

/// Filter out records with missing values in specific columns
pub fn filter_out_missing_values(
    batch: &RecordBatch,
    required_columns: &[&str],
) -> Result<RecordBatch> {
    // Create a mask where true means all required fields have values
    let num_rows = batch.num_rows();
    let mut has_all_values = vec![true; num_rows];

    for &column_name in required_columns {
        let col_idx = match batch.schema().index_of(column_name) {
            Ok(idx) => idx,
            Err(_) => {
                return Err(IdsError::Validation(format!(
                    "Required column '{column_name}' not found in batch"
                )));
            }
        };

        let column = batch.column(col_idx);

        // Update the mask
        for (i, has_value) in has_all_values.iter_mut().enumerate().take(num_rows) {
            if column.is_null(i) {
                *has_value = false;
            }
        }
    }

    let mask = BooleanArray::from(has_all_values);

    // Apply the filter to all columns
    let filtered_columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|col| filter_batch(col, &mask))
        .collect::<arrow::error::Result<_>>()
        .map_err(|e| IdsError::Data(format!("Failed to filter missing values: {e}")))?;

    // Create a new record batch with filtered data
    RecordBatch::try_new(batch.schema(), filtered_columns)
        .map_err(|e| IdsError::Data(format!("Failed to create filtered batch: {e}")))
}

/// Map categorical values in a string column based on a provided mapping
pub fn map_categorical_values(
    batch: &RecordBatch,
    column: &str,
    mapping: &HashMap<String, String>,
) -> Result<RecordBatch> {
    // Find the column index
    let col_idx = batch
        .schema()
        .index_of(column)
        .map_err(|e| IdsError::Validation(format!("Column '{column}' not found: {e}")))?;

    // Get the column and ensure it's a string column
    let string_array = batch.column(col_idx);
    let string_array = string_array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| IdsError::Validation(format!("Column '{column}' is not a string array")))?;

    // Create a new array with mapped values
    let mut mapped_values = Vec::with_capacity(batch.num_rows());

    for i in 0..string_array.len() {
        if string_array.is_null(i) {
            mapped_values.push(None);
        } else {
            let original_value = string_array.value(i);
            let mapped_value = mapping.get(original_value).map(std::string::String::as_str);
            mapped_values.push(mapped_value);
        }
    }

    // Create a new StringArray with the mapped values
    let mapped_array = StringArray::from(mapped_values);

    // Create a new schema by replacing the old field with a new one
    let schema = batch.schema();
    let mut fields = schema.fields().to_vec();

    // Replace the original field with the mapped field (keeping the same name)
    fields[col_idx] = Arc::new(Field::new(column, DataType::Utf8, true));
    let new_schema = Arc::new(Schema::new(fields));

    // Create a new record batch with the mapped column
    let mut columns = batch.columns().to_vec();
    columns[col_idx] = Arc::new(mapped_array);

    RecordBatch::try_new(new_schema, columns)
        .map_err(|e| IdsError::Data(format!("Failed to create batch with mapped values: {e}")))
}

/// Scale numeric values in a column by a scaling factor
pub fn scale_numeric_values(
    batch: &RecordBatch,
    column: &str,
    scale_factor: f64,
) -> Result<RecordBatch> {
    // Find the column index
    let col_idx = batch
        .schema()
        .index_of(column)
        .map_err(|e| IdsError::Validation(format!("Column '{column}' not found: {e}")))?;

    // Get the column
    let numeric_array = batch.column(col_idx);

    // Try to interpret the column as different numeric types
    let scaled_array: ArrayRef =
        if let Some(int_array) = numeric_array.as_any().downcast_ref::<Int32Array>() {
            // Scale Int32 values
            let mut scaled_values = Vec::with_capacity(batch.num_rows());

            for i in 0..int_array.len() {
                if int_array.is_null(i) {
                    scaled_values.push(None);
                } else {
                    let original_value = f64::from(int_array.value(i));
                    let scaled_value = original_value * scale_factor;
                    scaled_values.push(Some(scaled_value));
                }
            }

            Arc::new(Float64Array::from(scaled_values))
        } else if let Some(float_array) = numeric_array.as_any().downcast_ref::<Float64Array>() {
            // Scale Float64 values
            let mut scaled_values = Vec::with_capacity(batch.num_rows());

            for i in 0..float_array.len() {
                if float_array.is_null(i) {
                    scaled_values.push(None);
                } else {
                    let original_value = float_array.value(i);
                    let scaled_value = original_value * scale_factor;
                    scaled_values.push(Some(scaled_value));
                }
            }

            Arc::new(Float64Array::from(scaled_values))
        } else {
            return Err(IdsError::Validation(format!(
                "Column '{column}' is not a numeric array (Int32 or Float64)"
            )));
        };

    // Create a new schema with the scaled column as Float64
    let schema = batch.schema();
    let mut fields = schema.fields().to_vec();

    // Replace the original field with a Float64 field
    fields[col_idx] = Arc::new(Field::new(column, DataType::Float64, true));
    let new_schema = Arc::new(Schema::new(fields));

    // Create a new record batch with the scaled column
    let mut columns = batch.columns().to_vec();
    columns[col_idx] = scaled_array;

    RecordBatch::try_new(new_schema, columns)
        .map_err(|e| IdsError::Data(format!("Failed to create batch with scaled values: {e}")))
}

/// Group postal codes into regions and add a region column
pub fn add_postal_code_region(
    batch: &RecordBatch,
    postal_code_column: &str,
) -> Result<RecordBatch> {
    // Find the postal code column index
    let col_idx = batch.schema().index_of(postal_code_column).map_err(|e| {
        IdsError::Validation(format!("Column '{postal_code_column}' not found: {e}"))
    })?;

    // Get the postal code column
    let postal_code_array = batch.column(col_idx);
    let postal_code_array = postal_code_array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            IdsError::Validation(format!(
                "Column '{postal_code_column}' is not a string array"
            ))
        })?;

    // Create a new array with region values
    let mut region_values = Vec::with_capacity(batch.num_rows());

    for i in 0..postal_code_array.len() {
        if postal_code_array.is_null(i) {
            region_values.push(None);
        } else {
            let postal_code = postal_code_array.value(i);
            let region = determine_region_from_postal_code(postal_code);
            region_values.push(Some(region));
        }
    }

    // Create a new StringArray with the region values
    let region_array = StringArray::from(region_values);

    // Create a new field for the region column
    let region_field = Arc::new(Field::new("region", DataType::Utf8, true));

    // Create a new schema by adding the region field
    let schema = batch.schema();
    let mut fields = schema.fields().to_vec();
    fields.push(region_field);
    let new_schema = Arc::new(Schema::new(fields));

    // Create a new record batch with all the original columns plus the region column
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(region_array));

    RecordBatch::try_new(new_schema, columns)
        .map_err(|e| IdsError::Data(format!("Failed to create batch with region column: {e}")))
}

/// Determine Danish region from postal code
fn determine_region_from_postal_code(postal_code: &str) -> &'static str {
    if let Ok(code) = postal_code.parse::<u32>() {
        match code {
            1000..=2999 => "Hovedstaden",  // Copenhagen and surrounding areas
            3000..=3999 => "Nordsjælland", // North Zealand
            4000..=4999 => "Sjælland",     // Zealand
            5000..=5999 => "Fyn",          // Funen
            6000..=6999 => "Sydjylland",   // Southern Jutland
            7000..=7999 => "Midtjylland",  // Central Jutland
            8000..=8999 => "Østjylland",   // Eastern Jutland
            9000..=9999 => "Nordjylland",  // Northern Jutland
            _ => "Unknown",
        }
    } else {
        "Unknown"
    }
}
