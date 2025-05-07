//! Arrow utility functions for data type conversions and operations
//!
//! This module provides utility functions for working with Arrow arrays and data types,
//! with a focus on extracting and converting individual values from arrays.
//! It builds upon the type conversion functionality in the schema/adapt/conversions module.

use crate::error::{ParquetReaderError, Result};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array,
    Int32Array, Int64Array, StringArray,
};
use arrow::datatypes::DataType;
use chrono::NaiveDate;

/// Extract a string value from an Arrow array at the specified index, handling nulls
///
/// # Arguments
/// * `array` - The Arrow array
/// * `index` - The index of the value to extract
///
/// # Returns
/// `Some(String)` if the value exists and is not null, otherwise `None`
pub fn arrow_array_to_string(array: &ArrayRef, index: usize) -> Option<String> {
    if array.is_null(index) {
        return None;
    }

    match array.data_type() {
        DataType::Utf8 => {
            let string_array = array.as_any().downcast_ref::<StringArray>()?;
            Some(string_array.value(index).to_string())
        }
        // Handle other types that could be converted to strings
        _ => None,
    }
}

/// Extract a date value from an Arrow array at the specified index, handling nulls
///
/// # Arguments
/// * `array` - The Arrow array
/// * `index` - The index of the value to extract
///
/// # Returns
/// `Some(NaiveDate)` if the value exists and is not null, otherwise `None`
pub fn arrow_array_to_date(array: &ArrayRef, index: usize) -> Option<NaiveDate> {
    if array.is_null(index) {
        return None;
    }

    match array.data_type() {
        DataType::Date32 => {
            let date_array = array.as_any().downcast_ref::<Date32Array>()?;
            date_array.value_as_date(index)
        }
        DataType::Date64 => {
            let date_array = array.as_any().downcast_ref::<Date64Array>()?;
            date_array.value_as_date(index)
        }
        DataType::Utf8 => {
            let string_array = array.as_any().downcast_ref::<StringArray>()?;
            let date_str = string_array.value(index);

            // Try different date formats
            for format in &["%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d", "%d/%m/%Y"] {
                if let Ok(date) = NaiveDate::parse_from_str(date_str, format) {
                    return Some(date);
                }
            }

            None
        }
        _ => None,
    }
}

/// Extract an i32 value from an Arrow array at the specified index, handling nulls
///
/// # Arguments
/// * `array` - The Arrow array
/// * `index` - The index of the value to extract
///
/// # Returns
/// `Some(i32)` if the value exists and is not null, otherwise `None`
pub fn arrow_array_to_i32(array: &ArrayRef, index: usize) -> Option<i32> {
    if array.is_null(index) {
        return None;
    }

    match array.data_type() {
        DataType::Int32 => {
            let int_array = array.as_any().downcast_ref::<Int32Array>()?;
            Some(int_array.value(index))
        }
        DataType::Int64 => {
            let int_array = array.as_any().downcast_ref::<Int64Array>()?;
            Some(i32::try_from(int_array.value(index)).expect("Failed to cast i64 to i32"))
        }
        DataType::Float32 => {
            let float_array = array.as_any().downcast_ref::<Float32Array>()?;
            Some(float_array.value(index) as i32)
        }
        DataType::Float64 => {
            let float_array = array.as_any().downcast_ref::<Float64Array>()?;
            Some(float_array.value(index) as i32)
        }
        _ => None,
    }
}

/// Extract an i64 value from an Arrow array at the specified index, handling nulls
///
/// # Arguments
/// * `array` - The Arrow array
/// * `index` - The index of the value to extract
///
/// # Returns
/// `Some(i64)` if the value exists and is not null, otherwise `None`
pub fn arrow_array_to_i64(array: &ArrayRef, index: usize) -> Option<i64> {
    if array.is_null(index) {
        return None;
    }

    match array.data_type() {
        DataType::Int32 => {
            let int_array = array.as_any().downcast_ref::<Int32Array>()?;
            Some(i64::from(int_array.value(index)))
        }
        DataType::Int64 => {
            let int_array = array.as_any().downcast_ref::<Int64Array>()?;
            Some(int_array.value(index))
        }
        DataType::Float32 => {
            let float_array = array.as_any().downcast_ref::<Float32Array>()?;
            Some(float_array.value(index) as i64)
        }
        DataType::Float64 => {
            let float_array = array.as_any().downcast_ref::<Float64Array>()?;
            Some(float_array.value(index) as i64)
        }
        _ => None,
    }
}

/// Extract a float64 value from an Arrow array at the specified index, handling nulls
///
/// # Arguments
/// * `array` - The Arrow array
/// * `index` - The index of the value to extract
///
/// # Returns
/// `Some(f64)` if the value exists and is not null, otherwise `None`
pub fn arrow_array_to_f64(array: &ArrayRef, index: usize) -> Option<f64> {
    if array.is_null(index) {
        return None;
    }

    match array.data_type() {
        DataType::Int32 => {
            let int_array = array.as_any().downcast_ref::<Int32Array>()?;
            Some(f64::from(int_array.value(index)))
        }
        DataType::Int64 => {
            let int_array = array.as_any().downcast_ref::<Int64Array>()?;
            Some(int_array.value(index) as f64)
        }
        DataType::Float32 => {
            let float_array = array.as_any().downcast_ref::<Float32Array>()?;
            Some(f64::from(float_array.value(index)))
        }
        DataType::Float64 => {
            let float_array = array.as_any().downcast_ref::<Float64Array>()?;
            Some(float_array.value(index))
        }
        _ => None,
    }
}

/// Extract a boolean value from an Arrow array at the specified index, handling nulls
///
/// # Arguments
/// * `array` - The Arrow array
/// * `index` - The index of the value to extract
///
/// # Returns
/// `Some(bool)` if the value exists and is not null, otherwise `None`
pub fn arrow_array_to_bool(array: &ArrayRef, index: usize) -> Option<bool> {
    if array.is_null(index) {
        return None;
    }

    match array.data_type() {
        DataType::Boolean => {
            let bool_array = array.as_any().downcast_ref::<BooleanArray>()?;
            Some(bool_array.value(index))
        }
        _ => None,
    }
}

/// Get the column index by name from a record batch
///
/// # Arguments
/// * `batch` - The record batch
/// * `column_name` - The name of the column to find
///
/// # Returns
/// The index of the column
///
/// # Errors
/// Returns an error if the column does not exist
pub fn get_column_index(
    batch: &arrow::record_batch::RecordBatch,
    column_name: &str,
) -> Result<usize> {
    batch.schema().index_of(column_name).map_err(|_| {
        ParquetReaderError::ValidationError(format!("Column not found: {column_name}")).into()
    })
}

/// Get a column from a record batch by name
///
/// # Arguments
/// * `batch` - The record batch
/// * `column_name` - The name of the column to find
///
/// # Returns
/// The column as an `ArrayRef`
///
/// # Errors
/// Returns an error if the column does not exist
pub fn get_column(batch: &arrow::record_batch::RecordBatch, column_name: &str) -> Result<ArrayRef> {
    let idx = get_column_index(batch, column_name)?;
    Ok(batch.column(idx).clone())
}

/// Type-safe extraction of a `StringArray` from a column
///
/// # Arguments
/// * `batch` - The record batch
/// * `column_name` - The name of the column to extract
///
/// # Returns
/// The column as a `StringArray`
///
/// # Errors
/// Returns an error if the column does not exist or is not a `StringArray`
pub fn get_string_column<'a>(
    batch: &'a arrow::record_batch::RecordBatch,
    column_name: &str,
) -> Result<&'a StringArray> {
    let idx = get_column_index(batch, column_name)?;
    let column = batch.column(idx);

    column
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            ParquetReaderError::InvalidDataType {
                column: column_name.to_string(),
                expected: "StringArray".to_string(),
            }
            .into()
        })
}

/// Type-safe extraction of a `Date32Array` from a column
///
/// # Arguments
/// * `batch` - The record batch
/// * `column_name` - The name of the column to extract
///
/// # Returns
/// The column as a `Date32Array`
///
/// # Errors
/// Returns an error if the column does not exist or is not a `Date32Array`
pub fn get_date32_column<'a>(
    batch: &'a arrow::record_batch::RecordBatch,
    column_name: &str,
) -> Result<&'a Date32Array> {
    let idx = get_column_index(batch, column_name)?;
    let column = batch.column(idx);

    column
        .as_any()
        .downcast_ref::<Date32Array>()
        .ok_or_else(|| {
            ParquetReaderError::InvalidDataType {
                column: column_name.to_string(),
                expected: "Date32Array".to_string(),
            }
            .into()
        })
}

/// Type-safe extraction of a `Date64Array` from a column
///
/// # Arguments
/// * `batch` - The record batch
/// * `column_name` - The name of the column to extract
///
/// # Returns
/// The column as a `Date64Array`
///
/// # Errors
/// Returns an error if the column does not exist or is not a `Date64Array`
pub fn get_date64_column<'a>(
    batch: &'a arrow::record_batch::RecordBatch,
    column_name: &str,
) -> Result<&'a Date64Array> {
    let idx = get_column_index(batch, column_name)?;
    let column = batch.column(idx);

    column
        .as_any()
        .downcast_ref::<Date64Array>()
        .ok_or_else(|| {
            ParquetReaderError::InvalidDataType {
                column: column_name.to_string(),
                expected: "Date64Array".to_string(),
            }
            .into()
        })
}

/// Type-safe extraction of an `Int32Array` from a column
///
/// # Arguments
/// * `batch` - The record batch
/// * `column_name` - The name of the column to extract
///
/// # Returns
/// The column as an `Int32Array`
///
/// # Errors
/// Returns an error if the column does not exist or is not an `Int32Array`
pub fn get_int32_column<'a>(
    batch: &'a arrow::record_batch::RecordBatch,
    column_name: &str,
) -> Result<&'a Int32Array> {
    let idx = get_column_index(batch, column_name)?;
    let column = batch.column(idx);

    column.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
        ParquetReaderError::InvalidDataType {
            column: column_name.to_string(),
            expected: "Int32Array".to_string(),
        }
        .into()
    })
}
