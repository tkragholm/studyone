//! Module for converting between different array types.

use std::sync::Arc;
use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Date64Array, NullArray, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::compute::kernels::cast;
use arrow::datatypes::DataType;
use chrono::{DateTime, NaiveDate};

use crate::schema::adapt::types::{AdapterError, Result, DateFormatConfig};
use crate::schema::adapt::date_utils::parse_date_string;

/// Convert an Arrow array to match the target data type
pub fn convert_array(
    array: &ArrayRef,
    target_type: &DataType,
    date_config: &DateFormatConfig,
) -> Result<ArrayRef> {
    let source_type = array.data_type();

    // If types are already the same, return the array as-is
    if source_type == target_type {
        return Ok(array.clone());
    }

    match (source_type, target_type) {
        // String to Date32 conversion
        (DataType::Utf8 | DataType::LargeUtf8, &DataType::Date32) => {
            convert_string_to_date32(array, date_config)
        }

        // String to Date64 conversion
        (DataType::Utf8 | DataType::LargeUtf8, &DataType::Date64) => {
            convert_string_to_date64(array, date_config)
        }

        // String to Timestamp conversion
        (DataType::Utf8 | DataType::LargeUtf8, &DataType::Timestamp(unit, _)) => {
            convert_string_to_timestamp(array, &unit, date_config)
        }

        // Date32 to String conversion
        (&DataType::Date32, DataType::Utf8 | DataType::LargeUtf8) => {
            convert_date32_to_string(array, date_config)
        }

        // Date64 to String conversion
        (&DataType::Date64, DataType::Utf8 | DataType::LargeUtf8) => {
            convert_date64_to_string(array, date_config)
        }

        // Timestamp to String conversion
        (&DataType::Timestamp(unit, _), DataType::Utf8 | DataType::LargeUtf8) => {
            convert_timestamp_to_string(array, &unit, date_config)
        }

        // Between date types
        (&DataType::Date32, &DataType::Date64) => convert_date32_to_date64(array),

        (&DataType::Date64, &DataType::Date32) => convert_date64_to_date32(array),

        // Boolean to numeric conversion
        (&DataType::Boolean, t) if crate::schema::adapt::compatibility::is_numeric(t) => {
            // Use Arrow's cast functionality
            cast::cast(array, target_type).map_err(AdapterError::ArrowError)
        }

        // Boolean to string conversion
        (&DataType::Boolean, &DataType::Utf8 | &DataType::LargeUtf8) => {
            convert_boolean_to_string(array)
        }

        // Numeric type conversions (use Arrow's built-in casting)
        (s, t) if crate::schema::adapt::compatibility::is_numeric(s) && crate::schema::adapt::compatibility::is_numeric(t) => {
            cast::cast(array, target_type).map_err(AdapterError::ArrowError)
        }

        // Other types to string
        (_, &DataType::Utf8 | &DataType::LargeUtf8) => convert_to_string(array),

        // Default case - try to use Arrow cast when possible
        _ => cast::cast(array, target_type).map_err(|e| {
            AdapterError::ConversionError(format!(
                "Failed to convert from {source_type:?} to {target_type:?}: {e}"
            ))
        }),
    }
}

/// Create a null array of the specified type and length
pub fn create_null_array(data_type: &DataType, length: usize) -> Result<ArrayRef> {
    // For primitive types, use Arrow's built-in functions
    let null_array: ArrayRef = Arc::new(NullArray::new(length));
    match cast::cast(&null_array, data_type) {
        Ok(array) => Ok(array),
        Err(e) => Err(AdapterError::ConversionError(format!(
            "Failed to create null array of type {data_type:?}: {e}"
        ))),
    }
}

/// Convert a string array to a Date32 array
fn convert_string_to_date32(array: &ArrayRef, date_config: &DateFormatConfig) -> Result<ArrayRef> {
    // String array can be either Utf8 or LargeUtf8
    let string_array = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| AdapterError::ValidationError("Expected StringArray".to_string()))?;

    let mut builder = Date32Array::builder(string_array.len());

    for i in 0..string_array.len() {
        if string_array.is_null(i) {
            builder.append_null();
            continue;
        }

        let date_str = string_array.value(i);

        match parse_date_string(date_str, date_config) {
            Some(date) => {
                // Convert to days since epoch (1970-01-01)
                let days = date
                    .signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                    .num_days() as i32;
                builder.append_value(days);
            }
            None => {
                // If parsing fails, append null
                builder.append_null();
            }
        }
    }

    Ok(Arc::new(builder.finish()) as ArrayRef)
}

/// Convert a string array to Date64 array
fn convert_string_to_date64(array: &ArrayRef, date_config: &DateFormatConfig) -> Result<ArrayRef> {
    let string_array = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| AdapterError::ValidationError("Expected StringArray".to_string()))?;

    let mut builder = Date64Array::builder(string_array.len());

    for i in 0..string_array.len() {
        if string_array.is_null(i) {
            builder.append_null();
            continue;
        }

        let date_str = string_array.value(i);

        match parse_date_string(date_str, date_config) {
            Some(date) => {
                // Convert to milliseconds since epoch
                let millis = date
                    .and_hms_opt(0, 0, 0)
                    .unwrap()
                    .signed_duration_since(
                        NaiveDate::from_ymd_opt(1970, 1, 1)
                            .unwrap()
                            .and_hms_opt(0, 0, 0)
                            .unwrap(),
                    )
                    .num_milliseconds();
                builder.append_value(millis);
            }
            None => {
                // If parsing fails, append null
                builder.append_null();
            }
        }
    }

    Ok(Arc::new(builder.finish()) as ArrayRef)
}

/// Convert a string array to a Timestamp array with the specified unit
fn convert_string_to_timestamp(
    array: &ArrayRef,
    unit: &arrow::datatypes::TimeUnit,
    date_config: &DateFormatConfig,
) -> Result<ArrayRef> {
    // First convert to Date64 (milliseconds)
    let date64_array = convert_string_to_date64(array, date_config)?;

    // Then use Arrow's cast to convert to the right timestamp unit
    match unit {
        arrow::datatypes::TimeUnit::Second => cast::cast(
            &date64_array,
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None),
        )
        .map_err(AdapterError::ArrowError),
        arrow::datatypes::TimeUnit::Millisecond => cast::cast(
            &date64_array,
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
        )
        .map_err(AdapterError::ArrowError),
        arrow::datatypes::TimeUnit::Microsecond => cast::cast(
            &date64_array,
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
        )
        .map_err(AdapterError::ArrowError),
        arrow::datatypes::TimeUnit::Nanosecond => cast::cast(
            &date64_array,
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
        )
        .map_err(AdapterError::ArrowError),
    }
}

/// Convert a Date32 array to a string array
fn convert_date32_to_string(array: &ArrayRef, date_config: &DateFormatConfig) -> Result<ArrayRef> {
    let date_array = array
        .as_any()
        .downcast_ref::<Date32Array>()
        .ok_or_else(|| AdapterError::ValidationError("Expected Date32Array".to_string()))?;

    let format = &date_config.default_format;
    let mut string_builder = arrow::array::StringBuilder::new();

    for i in 0..date_array.len() {
        if date_array.is_null(i) {
            string_builder.append_null();
            continue;
        }

        let days = date_array.value(i);
        let date = NaiveDate::from_ymd_opt(1970, 1, 1)
            .unwrap()
            .checked_add_signed(chrono::Duration::days(i64::from(days)))
            .ok_or_else(|| AdapterError::ConversionError(format!("Invalid date value: {days}")))?;

        let formatted = date.format(format).to_string();
        string_builder.append_value(&formatted);
    }

    Ok(Arc::new(string_builder.finish()) as ArrayRef)
}

/// Convert a Date64 array to a string array
fn convert_date64_to_string(array: &ArrayRef, date_config: &DateFormatConfig) -> Result<ArrayRef> {
    let date_array = array
        .as_any()
        .downcast_ref::<Date64Array>()
        .ok_or_else(|| AdapterError::ValidationError("Expected Date64Array".to_string()))?;

    let format = &date_config.default_format;
    let mut string_builder = arrow::array::StringBuilder::new();

    for i in 0..date_array.len() {
        if date_array.is_null(i) {
            string_builder.append_null();
            continue;
        }

        let millis = date_array.value(i);
        let datetime =
            DateTime::from_timestamp(millis / 1000, ((millis % 1000) * 1_000_000) as u32)
                .ok_or_else(|| {
                    AdapterError::ConversionError(format!("Invalid date value: {millis}"))
                })?;

        let formatted = datetime.format(format).to_string();
        string_builder.append_value(&formatted);
    }

    Ok(Arc::new(string_builder.finish()) as ArrayRef)
}

/// Convert a timestamp array to a string array
fn convert_timestamp_to_string(
    array: &ArrayRef,
    unit: &arrow::datatypes::TimeUnit,
    date_config: &DateFormatConfig,
) -> Result<ArrayRef> {
    // Different handling based on the time unit
    let format = &date_config.default_format;
    let mut string_builder = arrow::array::StringBuilder::new();

    match unit {
        arrow::datatypes::TimeUnit::Second => {
            let ts_array = array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .ok_or_else(|| {
                    AdapterError::ValidationError("Expected TimestampSecondArray".to_string())
                })?;

            for i in 0..ts_array.len() {
                if ts_array.is_null(i) {
                    string_builder.append_null();
                    continue;
                }

                let seconds = ts_array.value(i);
                let datetime = DateTime::from_timestamp(seconds, 0).ok_or_else(|| {
                    AdapterError::ConversionError(format!("Invalid timestamp: {seconds}"))
                })?;

                let formatted = datetime.format(format).to_string();
                string_builder.append_value(&formatted);
            }
        }
        arrow::datatypes::TimeUnit::Millisecond => {
            let ts_array = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| {
                    AdapterError::ValidationError("Expected TimestampMillisecondArray".to_string())
                })?;

            for i in 0..ts_array.len() {
                if ts_array.is_null(i) {
                    string_builder.append_null();
                    continue;
                }

                let millis = ts_array.value(i);
                let datetime =
                    DateTime::from_timestamp(millis / 1000, ((millis % 1000) * 1_000_000) as u32)
                        .ok_or_else(|| {
                        AdapterError::ConversionError(format!("Invalid timestamp: {millis}"))
                    })?;

                let formatted = datetime.format(format).to_string();
                string_builder.append_value(&formatted);
            }
        }
        arrow::datatypes::TimeUnit::Microsecond => {
            let ts_array = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    AdapterError::ValidationError("Expected TimestampMicrosecondArray".to_string())
                })?;

            for i in 0..ts_array.len() {
                if ts_array.is_null(i) {
                    string_builder.append_null();
                    continue;
                }

                let micros = ts_array.value(i);
                let datetime = DateTime::from_timestamp(
                    micros / 1_000_000,
                    ((micros % 1_000_000) * 1000) as u32,
                )
                .ok_or_else(|| {
                    AdapterError::ConversionError(format!("Invalid timestamp: {micros}"))
                })?;

                let formatted = datetime.format(format).to_string();
                string_builder.append_value(&formatted);
            }
        }
        arrow::datatypes::TimeUnit::Nanosecond => {
            let ts_array = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| {
                    AdapterError::ValidationError("Expected TimestampNanosecondArray".to_string())
                })?;

            for i in 0..ts_array.len() {
                if ts_array.is_null(i) {
                    string_builder.append_null();
                    continue;
                }

                let nanos = ts_array.value(i);
                let datetime =
                    DateTime::from_timestamp(nanos / 1_000_000_000, (nanos % 1_000_000_000) as u32)
                        .ok_or_else(|| {
                            AdapterError::ConversionError(format!("Invalid timestamp: {nanos}"))
                        })?;

                let formatted = datetime.format(format).to_string();
                string_builder.append_value(&formatted);
            }
        }
    }

    Ok(Arc::new(string_builder.finish()) as ArrayRef)
}

/// Convert a Date32 array to Date64 array
fn convert_date32_to_date64(array: &ArrayRef) -> Result<ArrayRef> {
    let date32_array = array
        .as_any()
        .downcast_ref::<Date32Array>()
        .ok_or_else(|| AdapterError::ValidationError("Expected Date32Array".to_string()))?;

    let mut date64_builder = Date64Array::builder(date32_array.len());

    for i in 0..date32_array.len() {
        if date32_array.is_null(i) {
            date64_builder.append_null();
            continue;
        }

        let days = date32_array.value(i);
        // Convert days to milliseconds (86400000 ms per day)
        let millis = i64::from(days) * 86_400_000;
        date64_builder.append_value(millis);
    }

    Ok(Arc::new(date64_builder.finish()) as ArrayRef)
}

/// Convert a Date64 array to Date32 array
fn convert_date64_to_date32(array: &ArrayRef) -> Result<ArrayRef> {
    let date64_array = array
        .as_any()
        .downcast_ref::<Date64Array>()
        .ok_or_else(|| AdapterError::ValidationError("Expected Date64Array".to_string()))?;

    let mut date32_builder = Date32Array::builder(date64_array.len());

    for i in 0..date64_array.len() {
        if date64_array.is_null(i) {
            date32_builder.append_null();
            continue;
        }

        let millis = date64_array.value(i);
        // Convert milliseconds to days (86400000 ms per day)
        let days = (millis / 86_400_000) as i32;
        date32_builder.append_value(days);
    }

    Ok(Arc::new(date32_builder.finish()) as ArrayRef)
}

/// Convert a boolean array to a string array
fn convert_boolean_to_string(array: &ArrayRef) -> Result<ArrayRef> {
    let bool_array = array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| AdapterError::ValidationError("Expected BooleanArray".to_string()))?;

    let mut string_builder = arrow::array::StringBuilder::new();

    for i in 0..bool_array.len() {
        if bool_array.is_null(i) {
            string_builder.append_null();
            continue;
        }

        let value = bool_array.value(i);
        string_builder.append_value(if value { "true" } else { "false" });
    }

    Ok(Arc::new(string_builder.finish()) as ArrayRef)
}

/// Convert any array to a string array using debug formatting
fn convert_to_string(array: &ArrayRef) -> Result<ArrayRef> {
    // Use Arrow's cast for types that are easily converted to strings
    if let Ok(string_array) = cast::cast(array, &DataType::Utf8) {
        return Ok(string_array);
    }

    // Fall back to manual conversion using debug formatting
    let mut string_builder = arrow::array::StringBuilder::new();

    for i in 0..array.len() {
        if array.is_null(i) {
            string_builder.append_null();
            continue;
        }

        // Use the array debug format for the value
        let value = format!("{:?}", array);
        string_builder.append_value(&value);
    }

    Ok(Arc::new(string_builder.finish()) as ArrayRef)
}