//! Data transformation utilities for registry data
//!
//! This module provides functionality for filtering and transforming Arrow record batches.

use crate::error::ResultExt;
use anyhow::Context;
use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float64Array, Int32Array, StringArray,
};
use arrow::compute::kernels::cmp;
use arrow::compute::{filter as filter_batch, kernels};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{Datelike, NaiveDate};
use std::collections::HashMap;
use std::sync::Arc;

use crate::error::{Error, Result};

/// Transform multiple record batches using a provided transformation function
pub fn transform_records(
    batches: &[RecordBatch],
    transformation: &(dyn Fn(&RecordBatch) -> Result<RecordBatch> + Send + Sync),
) -> Result<Vec<RecordBatch>> {
    use rayon::prelude::*;

    // Use parallel processing for transformation
    let results: Vec<Result<RecordBatch>> = batches
        .par_iter()
        .map(|batch| transformation(batch))
        .collect();

    // Filter out empty batches and collect results
    let mut transformed_batches = Vec::with_capacity(batches.len());
    for result in results {
        match result {
            Ok(batch) if batch.num_rows() > 0 => transformed_batches.push(batch),
            Ok(_) => {} // Skip empty batches
            Err(e) => return Err(e),
        }
    }

    Ok(transformed_batches)
}

/// Filter record batches by date range
pub fn filter_by_date_range(
    batch: &RecordBatch,
    date_column: &str,
    start_date: Option<NaiveDate>,
    end_date: Option<NaiveDate>,
) -> Result<RecordBatch> {
    // Find the date column
    let date_idx = batch.schema().index_of(date_column).map_err(|e| {
        Error::ValidationError(format!("Date column '{date_column}' not found: {e}"))
    })?;

    let date_array = batch.column(date_idx);
    let date_array = date_array
        .as_any()
        .downcast_ref::<Date32Array>()
        .ok_or_else(|| {
            Error::ValidationError(format!("Column '{date_column}' is not a Date32 array"))
        })?;

    // Use Arrow's compute functions for vectorized comparison
    let mut in_range = BooleanArray::from(vec![true; batch.num_rows()]);

    // Apply start date filter if specified
    if let Some(start) = start_date {
        let start_days = start.num_days_from_ce() - 719163;

        // Create array of the start date for vectorized comparison
        let start_date_array = Date32Array::from(vec![start_days; batch.num_rows()]);

        // Vectorized comparison: date >= start_date
        let ge_result =
            cmp::gt_eq(date_array, &start_date_array).with_context(|| "Failed to compare dates")?;

        // Combine with existing mask
        in_range = kernels::boolean::and(&in_range, &ge_result)
            .with_context(|| "Failed to combine date filters")?;
    }

    // Apply end date filter if specified
    if let Some(end) = end_date {
        let end_days = end.num_days_from_ce() - 719163;

        // Create array of the end date for vectorized comparison
        let end_date_array = Date32Array::from(vec![end_days; batch.num_rows()]);

        // Vectorized comparison: date <= end_date
        let le_result =
            cmp::lt_eq(date_array, &end_date_array).with_context(|| "Failed to compare dates")?;

        // Combine with existing mask
        in_range = kernels::boolean::and(&in_range, &le_result)
            .with_context(|| "Failed to combine date filters")?;
    }

    // Handle nulls in the date column - exclude rows with null dates
    // Create a boolean array where true means the date is not null
    let mut not_null_values = Vec::with_capacity(date_array.len());
    for i in 0..date_array.len() {
        not_null_values.push(!date_array.is_null(i));
    }
    let null_mask = arrow::array::BooleanArray::from(not_null_values);

    // Combine with date range mask
    let mask = kernels::boolean::and(&in_range, &null_mask).with_context(|| "Failed to combine masks")?;

    // Apply the filter to all columns
    let filtered_columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|col| filter_batch(col, &mask))
        .collect::<arrow::error::Result<_>>()
        .with_context(|| "Failed to filter batch by date range")?;

    // Create a new record batch with filtered data
    RecordBatch::try_new(batch.schema(), filtered_columns)
        .with_context(|| "Failed to create filtered batch")
}

/// Extract year from date column and add it as a new column
pub fn add_year_column(batch: &RecordBatch, date_column: &str) -> Result<RecordBatch> {
    // Find the date column
    let date_idx = batch.schema().index_of(date_column).map_err(|e| {
        Error::ValidationError(format!("Date column '{date_column}' not found: {e}"))
    })?;

    let date_array = batch.column(date_idx);
    let date_array = date_array
        .as_any()
        .downcast_ref::<Date32Array>()
        .ok_or_else(|| {
            Error::ValidationError(format!("Column '{date_column}' is not a Date32 array"))
        })?;

    // Create a new Int32Array with year values - use arrow computation logic
    let mut year_values = Vec::with_capacity(batch.num_rows());

    // Implement this vectorized if Arrow provides a function, otherwise do it per-element
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
        Error::ArrowError(format!(
            "Failed to create record batch with year column: {e}"
        ))
        .into()
    })
}

/// Filter out records with missing values in specific columns
pub fn filter_out_missing_values(
    batch: &RecordBatch,
    required_columns: &[&str],
) -> Result<RecordBatch> {
    // Start with a mask where all rows are included
    let mut mask = BooleanArray::from(vec![true; batch.num_rows()]);

    // Check each required column
    for &column_name in required_columns {
        let col_idx = match batch.schema().index_of(column_name) {
            Ok(idx) => idx,
            Err(_) => {
                return Err(Error::ValidationError(format!(
                    "Required column '{column_name}' not found in batch"
                ))
                .into());
            }
        };

        let column = batch.column(col_idx);

        // Create a mask where true means non-null values
        let null_bitmap = column.nulls();
        let is_valid_array = match null_bitmap {
            Some(_bitmap) => {
                // If we have a null bitmap, create a boolean array from validity
                let mut builder = arrow::array::BooleanBuilder::new();
                for i in 0..column.len() {
                    builder.append_value(!column.is_null(i));
                }
                builder.finish()
            },
            None => {
                // If there are no nulls, all values are valid
                let values = vec![true; column.len()];
                arrow::array::BooleanArray::from(values)
            }
        };
        
        // Update the overall mask to include only rows where all required fields are non-null
        mask =
            kernels::boolean::and(&mask, &is_valid_array).with_context(|| "Failed to combine null masks")?;
    }

    // Apply the filter to all columns
    let filtered_columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|col| filter_batch(col, &mask))
        .collect::<arrow::error::Result<_>>()
        .with_context(|| "Failed to filter missing values")?;

    // Create a new record batch with filtered data
    RecordBatch::try_new(batch.schema(), filtered_columns)
        .with_context(|| "Failed to create filtered batch")
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
        .with_context(|| format!("Column '{}' not found", column))?;

    // Get the column and ensure it's a string column
    let string_array = batch.column(col_idx);
    let string_array = string_array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            Error::ValidationError(format!("Column '{column}' is not a string array"))
        })?;

    // Create a new array with mapped values using itertools for efficiency
    let mapped_values = (0..string_array.len())
        .map(|i| {
            if string_array.is_null(i) {
                None
            } else {
                let original_value = string_array.value(i);
                mapping.get(original_value).map(Clone::clone)
            }
        })
        .collect::<Vec<_>>();

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

    RecordBatch::try_new(new_schema, columns).with_context(|| "Failed to create batch with mapped values")
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
        .with_context(|| format!("Column '{}' not found", column))?;

    // Get the column
    let numeric_array = batch.column(col_idx);

    // Try to interpret the column as different numeric types and use Arrow compute functions
    let scaled_array: ArrayRef =
        if let Some(int_array) = numeric_array.as_any().downcast_ref::<Int32Array>() {
            // Convert Int32 to Float64 first
            let float_array = kernels::cast::cast(int_array, &DataType::Float64)
                .with_context(|| "Failed to cast Int32 to Float64")?;

            // Now multiply by scale factor - use kernel if available or implement efficiently
            let float_array = float_array.as_any().downcast_ref::<Float64Array>().unwrap();

            // Vectorized multiplication using itertools for efficiency
            let scaled_values = (0..float_array.len())
                .map(|i| {
                    if float_array.is_null(i) {
                        None
                    } else {
                        Some(float_array.value(i) * scale_factor)
                    }
                })
                .collect::<Vec<_>>();

            Arc::new(Float64Array::from(scaled_values))
        } else if let Some(float_array) = numeric_array.as_any().downcast_ref::<Float64Array>() {
            // Directly scale Float64 values
            let scaled_values = (0..float_array.len())
                .map(|i| {
                    if float_array.is_null(i) {
                        None
                    } else {
                        Some(float_array.value(i) * scale_factor)
                    }
                })
                .collect::<Vec<_>>();

            Arc::new(Float64Array::from(scaled_values))
        } else {
            return Err(Error::ValidationError(format!(
                "Column '{column}' is not a numeric array (Int32 or Float64)"
            ))
            .into());
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

    RecordBatch::try_new(new_schema, columns).with_msg("Failed to create batch with scaled values")
}

/// Group postal codes into regions and add a region column
pub fn add_postal_code_region(
    batch: &RecordBatch,
    postal_code_column: &str,
) -> Result<RecordBatch> {
    // Find the postal code column index
    let col_idx = batch.schema().index_of(postal_code_column).map_err(|e| {
        Error::ValidationError(format!("Column '{postal_code_column}' not found: {e}"))
    })?;

    // Get the postal code column
    let postal_code_array = batch.column(col_idx);
    let postal_code_array = postal_code_array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            Error::ValidationError(format!(
                "Column '{postal_code_column}' is not a string array"
            ))
        })?;

    // Create a new array with region values using itertools for efficiency
    let region_values = (0..postal_code_array.len())
        .map(|i| {
            if postal_code_array.is_null(i) {
                None
            } else {
                let postal_code = postal_code_array.value(i);
                Some(determine_region_from_postal_code(postal_code).to_string())
            }
        })
        .collect::<Vec<_>>();

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

    RecordBatch::try_new(new_schema, columns).with_context(|| "Failed to create batch with region column")
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
