//! Arrow utility functions for data type conversions and operations
//!
//! This module provides utility functions for working with Arrow arrays and data types,
//! with a focus on extracting and converting individual values from arrays.
//! It builds upon the type conversion functionality in the schema/adapt/conversions module.

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

/// Convert Arrow Date32 value to `NaiveDate`
#[must_use]
pub fn arrow_date_to_naive_date(days_since_epoch: i32) -> NaiveDate {
    // Using a non-const approach for the date calculation
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    epoch
        .checked_add_days(chrono::Days::new(days_since_epoch as u64))
        .unwrap_or(epoch)
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
