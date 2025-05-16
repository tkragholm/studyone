//! Field extraction utilities for Arrow record batches
//!
//! This module provides high-level utilities for extracting typed field values
//! from Arrow record batches with appropriate error handling and type conversion.

use crate::error::Result;
use crate::utils::arrow::array_utils::{downcast_array, get_column};
use arrow::array::{
    Array, BooleanArray, Date32Array, Float64Array, Int8Array, Int32Array, StringArray,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;

/// Extract a string value from a record batch
///
/// # Arguments
///
/// * `batch` - The record batch to extract from
/// * `row` - The row index
/// * `column_name` - The name of the column
/// * `required` - Whether the column is required
///
/// # Returns
///
/// * `Ok(Some(String))` - The extracted string value
/// * `Ok(None)` - If the column is null or not present (and not required)
/// * `Err` - If an error occurs during extraction
pub fn extract_string(
    batch: &RecordBatch,
    row: usize,
    column_name: &str,
    required: bool,
) -> Result<Option<String>> {
    let array_opt = get_column(batch, column_name, &DataType::Utf8, required)?;

    if let Some(array) = array_opt {
        let string_array = downcast_array::<StringArray>(&array, column_name, "String")?;

        if row < string_array.len() && !string_array.is_null(row) {
            let value = string_array.value(row).to_string();
            if !value.is_empty() {
                return Ok(Some(value));
            }
        }
    }

    Ok(None)
}

/// Extract a date value from a record batch (Date32 format)
///
/// # Arguments
///
/// * `batch` - The record batch to extract from
/// * `row` - The row index
/// * `column_name` - The name of the column
/// * `required` - Whether the column is required
///
/// # Returns
///
/// * `Ok(Some(NaiveDate))` - The extracted date value
/// * `Ok(None)` - If the column is null or not present (and not required)
///
/// # Errors
///
/// Returns an error if:
/// - The column cannot be retrieved
/// - The column cannot be downcast to the expected array type
///
/// # Panics
///
/// This function does not panic, as it uses `.from_ymd_opt()` instead of `.unwrap()`
pub fn extract_date32(
    batch: &RecordBatch,
    row: usize,
    column_name: &str,
    required: bool,
) -> Result<Option<NaiveDate>> {
    let array_opt = get_column(batch, column_name, &DataType::Date32, required)?;

    if let Some(array) = array_opt {
        let date_array = downcast_array::<Date32Array>(&array, column_name, "Date32")?;

        if row < date_array.len() && !date_array.is_null(row) {
            let days_since_epoch = date_array.value(row);
            return Ok(
                chrono::NaiveDate::from_ymd_opt(1970, 1, 1).and_then(|base_date| {
                    base_date.checked_add_days(chrono::Days::new(days_since_epoch as u64))
                }),
            );
        }
    }

    Ok(None)
}

/// Extract a date value from a string in YYYYMMDD format
///
/// # Arguments
///
/// * `batch` - The record batch to extract from
/// * `row` - The row index
/// * `column_name` - The name of the column
/// * `required` - Whether the column is required
///
/// # Returns
///
/// * `Ok(Some(NaiveDate))` - The extracted date value
/// * `Ok(None)` - If the column is null or not present (and not required)
/// * `Err` - If an error occurs during extraction
pub fn extract_date_from_string(
    batch: &RecordBatch,
    row: usize,
    column_name: &str,
    required: bool,
) -> Result<Option<NaiveDate>> {
    if let Some(date_str) = extract_string(batch, row, column_name, required)? {
        if date_str.len() == 8 {
            if let (Ok(year), Ok(month), Ok(day)) = (
                date_str[0..4].parse::<i32>(),
                date_str[4..6].parse::<u32>(),
                date_str[6..8].parse::<u32>(),
            ) {
                return Ok(NaiveDate::from_ymd_opt(year, month, day));
            }
        }
    }

    Ok(None)
}

/// Extract an int8 value and format it as a string with leading zeros
///
/// # Arguments
///
/// * `batch` - The record batch to extract from
/// * `row` - The row index
/// * `column_name` - The name of the column
/// * `required` - Whether the column is required
/// * `padding` - The number of digits to pad to (e.g., 3 for "001")
///
/// # Returns
///
/// * `Ok(Some(String))` - The extracted and formatted value
/// * `Ok(None)` - If the column is null or not present (and not required)
/// * `Err` - If an error occurs during extraction
pub fn extract_int8_as_padded_string(
    batch: &RecordBatch,
    row: usize,
    column_name: &str,
    required: bool,
    padding: usize,
) -> Result<Option<String>> {
    let array_opt = get_column(batch, column_name, &DataType::Int8, required)?;

    if let Some(array) = array_opt {
        let int_array = downcast_array::<Int8Array>(&array, column_name, "Int8")?;

        if row < int_array.len() && !int_array.is_null(row) {
            let value = int_array.value(row);
            return Ok(Some(format!("{value:0padding$}")));
        }
    }

    Ok(None)
}

/// Extract an int32 value from a record batch
///
/// # Arguments
///
/// * `batch` - The record batch to extract from
/// * `row` - The row index
/// * `column_name` - The name of the column
/// * `required` - Whether the column is required
///
/// # Returns
///
/// * `Ok(Some(i32))` - The extracted integer value
/// * `Ok(None)` - If the column is null or not present (and not required)
/// * `Err` - If an error occurs during extraction
pub fn extract_int32(
    batch: &RecordBatch,
    row: usize,
    column_name: &str,
    required: bool,
) -> Result<Option<i32>> {
    let array_opt = get_column(batch, column_name, &DataType::Int32, required)?;

    if let Some(array) = array_opt {
        let int_array = downcast_array::<Int32Array>(&array, column_name, "Int32")?;

        if row < int_array.len() && !int_array.is_null(row) {
            return Ok(Some(int_array.value(row)));
        }
    }

    // Try with Int8 if Int32 is not available
    let array_opt = get_column(batch, column_name, &DataType::Int8, required)?;

    if let Some(array) = array_opt {
        let int_array = downcast_array::<Int8Array>(&array, column_name, "Int8")?;

        if row < int_array.len() && !int_array.is_null(row) {
            return Ok(Some(i32::from(int_array.value(row))));
        }
    }

    Ok(None)
}

/// Extract a boolean value from a record batch
///
/// # Arguments
///
/// * `batch` - The record batch to extract from
/// * `row` - The row index
/// * `column_name` - The name of the column
/// * `required` - Whether the column is required
///
/// # Returns
///
/// * `Ok(Some(bool))` - The extracted boolean value
/// * `Ok(None)` - If the column is null or not present (and not required)
/// * `Err` - If an error occurs during extraction
pub fn extract_boolean(
    batch: &RecordBatch,
    row: usize,
    column_name: &str,
    required: bool,
) -> Result<Option<bool>> {
    let array_opt = get_column(batch, column_name, &DataType::Boolean, required)?;

    if let Some(array) = array_opt {
        let bool_array = downcast_array::<BooleanArray>(&array, column_name, "Boolean")?;

        if row < bool_array.len() && !bool_array.is_null(row) {
            return Ok(Some(bool_array.value(row)));
        }
    }

    Ok(None)
}

/// Extract a float64 value from a record batch
///
/// # Arguments
///
/// * `batch` - The record batch to extract from
/// * `row` - The row index
/// * `column_name` - The name of the column
/// * `required` - Whether the column is required
///
/// # Returns
///
/// * `Ok(Some(f64))` - The extracted float value
/// * `Ok(None)` - If the column is null or not present (and not required)
/// * `Err` - If an error occurs during extraction
pub fn extract_float64(
    batch: &RecordBatch,
    row: usize,
    column_name: &str,
    required: bool,
) -> Result<Option<f64>> {
    let array_opt = get_column(batch, column_name, &DataType::Float64, required)?;

    if let Some(array) = array_opt {
        let float_array = downcast_array::<Float64Array>(&array, column_name, "Float64")?;

        if row < float_array.len() && !float_array.is_null(row) {
            return Ok(Some(float_array.value(row)));
        }
    }

    Ok(None)
}

/// Extract a field value using a custom extraction function
///
/// This is a more generic version that allows custom transformation of the extracted value.
///
/// # Type Parameters
///
/// * `A` - The Arrow array type
/// * `T` - The output type
/// * `F` - The transformation function type
///
/// # Arguments
///
/// * `batch` - The record batch to extract from
/// * `row` - The row index
/// * `column_name` - The name of the column
/// * `data_type` - The expected Arrow data type
/// * `type_name` - A string representation of the expected type (for error messages)
/// * `required` - Whether the column is required
/// * `transform` - A function to transform the value if present
///
/// # Returns
///
/// * `Ok(Some(T))` - The extracted and transformed value
/// * `Ok(None)` - If the column is null or not present (and not required)
/// * `Err` - If an error occurs during extraction
pub fn extract_with_transform<A, T, F>(
    batch: &RecordBatch,
    row: usize,
    column_name: &str,
    data_type: &DataType,
    type_name: &str,
    required: bool,
    transform: F,
) -> Result<Option<T>>
where
    A: Array + 'static,
    F: FnOnce(&A, usize) -> Option<T>,
{
    let array_opt = get_column(batch, column_name, data_type, required)?;

    if let Some(array) = array_opt {
        let typed_array = downcast_array::<A>(&array, column_name, type_name)?;

        if row < typed_array.len() && !typed_array.is_null(row) {
            return Ok(transform(typed_array, row));
        }
    }

    Ok(None)
}