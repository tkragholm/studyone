//! Module for handling data type adaptation between mismatched schemas.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Date64Array, NullArray, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::compute::kernels::cast;
use arrow::datatypes::{DataType, Schema};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, NaiveDate};

/// Errors that can occur during type adaptation
#[derive(Debug, thiserror::Error)]
pub enum AdapterError {
    /// Arrow error
    #[error("Arrow error: {0}")]
    ArrowError(#[from] ArrowError),

    /// Error during type conversion
    #[error("Type conversion error: {0}")]
    ConversionError(String),

    /// Date parsing error
    #[error("Date parsing error: {0}")]
    DateParsingError(String),

    /// Validation error
    #[error("Validation error: {0}")]
    ValidationError(String),
}

/// Alias for Result with `AdapterError`
pub type Result<T> = std::result::Result<T, AdapterError>;

/// Types of data type compatibility
#[derive(Debug, PartialEq, Eq)]
pub enum TypeCompatibility {
    /// Types match exactly
    Exact,
    /// Types can be automatically converted
    Compatible,
    /// Types are incompatible
    Incompatible,
}

/// Check if two Arrow data types are compatible for conversion
#[must_use]
pub fn check_type_compatibility(from: &DataType, to: &DataType) -> TypeCompatibility {
    if from == to {
        return TypeCompatibility::Exact;
    }

    match (from, to) {
        // Numeric type conversions (widening)
        (DataType::Int8, DataType::Int16 | DataType::Int32 | DataType::Int64)
        | (DataType::Int16, DataType::Int32 | DataType::Int64)
        | (DataType::Int32, DataType::Int64)
        | (DataType::UInt8, DataType::UInt16 | DataType::UInt32 | DataType::UInt64)
        | (DataType::UInt16, DataType::UInt32 | DataType::UInt64)
        | (DataType::UInt32, DataType::UInt64)
        | (DataType::Float32, DataType::Float64) => TypeCompatibility::Compatible,

        // Integer to float conversions
        (
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32,
            DataType::Float32,
        )
        | (
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64,
            DataType::Float64,
        ) => TypeCompatibility::Compatible,

        // String-convertible types
        (
            DataType::Utf8 | DataType::LargeUtf8,
            DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _),
        ) => TypeCompatibility::Compatible,

        // Date/Timestamp to String conversions
        (
            DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _),
            DataType::Utf8 | DataType::LargeUtf8,
        ) => TypeCompatibility::Compatible,

        // Date and timestamp interconversions
        (DataType::Date32 | DataType::Timestamp(_, _), DataType::Date64)
        | (DataType::Date32 | DataType::Date64, DataType::Timestamp(_, _))
        | (DataType::Date64 | DataType::Timestamp(_, _), DataType::Date32) => {
            TypeCompatibility::Compatible
        }

        // Between string types
        (DataType::Utf8, DataType::LargeUtf8) | (DataType::LargeUtf8, DataType::Utf8) => {
            TypeCompatibility::Compatible
        }

        // Boolean conversions
        (
            DataType::Boolean,
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Utf8
            | DataType::LargeUtf8,
        ) => TypeCompatibility::Compatible,

        // Default - Incompatible
        _ => TypeCompatibility::Incompatible,
    }
}

/// Identifies whether a data type is numeric
#[must_use]
pub const fn is_numeric(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
    )
}

/// Identifies whether a data type is a string type
#[must_use]
pub const fn is_string(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Utf8 | DataType::LargeUtf8)
}

/// Identifies whether a data type is a date or timestamp type
#[must_use]
pub const fn is_temporal(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _)
    )
}

/// An enhanced schema compatibility report with adaptation information
#[derive(Debug)]
pub struct EnhancedSchemaCompatibilityReport {
    /// Whether all schemas are compatible with adaptation
    pub compatible: bool,
    /// List of incompatibility issues that cannot be resolved with adaptation
    pub issues: Vec<SchemaAdaptationIssue>,
    /// List of adaptations that will be performed
    pub adaptations: Vec<SchemaAdaptation>,
}

/// A schema compatibility issue with adaptation context
#[derive(Debug)]
pub struct SchemaAdaptationIssue {
    /// The field name with incompatibility
    pub field_name: String,
    /// The source data type
    pub source_type: DataType,
    /// The target data type
    pub target_type: DataType,
    /// Description of the incompatibility
    pub description: String,
}

/// A schema adaptation to be performed
#[derive(Debug)]
pub struct SchemaAdaptation {
    /// The field name to adapt
    pub field_name: String,
    /// The source data type
    pub source_type: DataType,
    /// The target data type
    pub target_type: DataType,
    /// The strategy for adaptation
    pub adaptation_strategy: AdaptationStrategy,
}

/// Available strategies for type adaptation
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdaptationStrategy {
    /// Automatically cast using Arrow's cast functionality
    AutoCast,
    /// Parse date strings into date types
    DateParsing,
    /// Convert dates/timestamps to strings
    DateFormatting,
    /// Convert to string representation
    StringConversion,
    /// Convert numeric types (widening)
    NumericConversion,
    /// Convert boolean values
    BooleanConversion,
}

/// Check schema compatibility with adaptation options
#[must_use]
pub fn check_schema_with_adaptation(
    source_schema: &Schema,
    target_schema: &Schema,
) -> EnhancedSchemaCompatibilityReport {
    let mut issues = Vec::new();
    let mut adaptations = Vec::new();
    let mut all_compatible = true;

    // Check each field in the target schema
    for target_field in target_schema.fields() {
        let field_name = target_field.name();

        // Try to find matching field in source schema
        if let Ok(source_field) = source_schema.field_with_name(field_name) {
            let source_type = source_field.data_type();
            let target_type = target_field.data_type();

            // Check compatibility
            match check_type_compatibility(source_type, target_type) {
                TypeCompatibility::Exact => {
                    // Types match exactly, no adaptation needed
                }
                TypeCompatibility::Compatible => {
                    // Types need adaptation
                    let strategy = determine_adaptation_strategy(source_type, target_type);
                    adaptations.push(SchemaAdaptation {
                        field_name: field_name.to_string(),
                        source_type: source_type.clone(),
                        target_type: target_type.clone(),
                        adaptation_strategy: strategy,
                    });
                }
                TypeCompatibility::Incompatible => {
                    // Types are incompatible
                    all_compatible = false;
                    issues.push(SchemaAdaptationIssue {
                        field_name: field_name.to_string(),
                        source_type: source_type.clone(),
                        target_type: target_type.clone(),
                        description: format!(
                            "Incompatible types for field '{field_name}': {source_type:?} cannot be converted to {target_type:?}"
                        ),
                    });
                }
            }
        } else {
            // Field doesn't exist in source schema
            // This is not an adaptation issue, as we can create null values
            // But we should note it for informational purposes
            adaptations.push(SchemaAdaptation {
                field_name: field_name.to_string(),
                source_type: DataType::Null,
                target_type: target_field.data_type().clone(),
                adaptation_strategy: AdaptationStrategy::AutoCast,
            });
        }
    }

    EnhancedSchemaCompatibilityReport {
        compatible: all_compatible,
        issues,
        adaptations,
    }
}

/// Determine the appropriate adaptation strategy for a given source and target type
const fn determine_adaptation_strategy(
    source_type: &DataType,
    target_type: &DataType,
) -> AdaptationStrategy {
    match (source_type, target_type) {
        // String to date conversions
        (s, t) if is_string(s) && is_temporal(t) => AdaptationStrategy::DateParsing,

        // Date to string conversions
        (s, t) if is_temporal(s) && is_string(t) => AdaptationStrategy::DateFormatting,

        // Numeric conversions
        (s, t) if is_numeric(s) && is_numeric(t) => AdaptationStrategy::NumericConversion,

        // Boolean conversions
        (DataType::Boolean, _) => AdaptationStrategy::BooleanConversion,

        // String conversions for other types
        (_, DataType::Utf8 | DataType::LargeUtf8) => AdaptationStrategy::StringConversion,

        // Default to auto cast
        _ => AdaptationStrategy::AutoCast,
    }
}

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
        (&DataType::Boolean, t) if is_numeric(t) => {
            // Use Arrow's cast functionality
            cast::cast(array, target_type).map_err(AdapterError::ArrowError)
        }

        // Boolean to string conversion
        (&DataType::Boolean, &DataType::Utf8 | &DataType::LargeUtf8) => {
            convert_boolean_to_string(array)
        }

        // Numeric type conversions (use Arrow's built-in casting)
        (s, t) if is_numeric(s) && is_numeric(t) => {
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

/// Convert a record batch to match the target schema with type adaptation
pub fn adapt_record_batch(
    batch: &RecordBatch,
    target_schema: &Schema,
    date_config: &DateFormatConfig,
) -> Result<RecordBatch> {
    let source_schema = batch.schema();
    let mut adapted_columns: Vec<ArrayRef> = Vec::with_capacity(target_schema.fields().len());

    // Process each field in the target schema
    for target_field in target_schema.fields() {
        let field_name = target_field.name();
        let target_type = target_field.data_type();

        // Try to find the field in the source schema
        if let Ok(source_idx) = source_schema.index_of(field_name) {
            let source_array = batch.column(source_idx);
            let source_type = source_array.data_type();

            match check_type_compatibility(source_type, target_type) {
                TypeCompatibility::Exact => {
                    // Types match, use column as-is
                    adapted_columns.push(source_array.clone());
                }
                TypeCompatibility::Compatible => {
                    // Types need conversion
                    let converted = convert_array(source_array, target_type, date_config)?;
                    adapted_columns.push(converted);
                }
                TypeCompatibility::Incompatible => {
                    return Err(AdapterError::ValidationError(format!(
                        "Incompatible types for field '{field_name}': {source_type:?} -> {target_type:?}"
                    )));
                }
            }
        } else {
            // Field doesn't exist in source schema, create a null column
            let null_array = create_null_array(target_type, batch.num_rows())?;
            adapted_columns.push(null_array);
        }
    }

    // Create a new record batch with the adapted columns
    RecordBatch::try_new(Arc::new(target_schema.clone()), adapted_columns)
        .map_err(AdapterError::ArrowError)
}

/// Create a null array of the specified type and length
fn create_null_array(data_type: &DataType, length: usize) -> Result<ArrayRef> {
    // For primitive types, use Arrow's built-in functions
    let null_array: ArrayRef = Arc::new(NullArray::new(length));
    match cast::cast(&null_array, data_type) {
        Ok(array) => Ok(array),
        Err(e) => Err(AdapterError::ConversionError(format!(
            "Failed to create null array of type {data_type:?}: {e}"
        ))),
    }
}

/// Configuration for date format handling
#[derive(Debug, Clone)]
pub struct DateFormatConfig {
    /// List of date format strings to try when parsing dates
    pub date_formats: Vec<String>,
    /// Default date format to use when converting dates to strings
    pub default_format: String,
    /// Enable heuristic format detection
    pub enable_format_detection: bool,
}

impl Default for DateFormatConfig {
    fn default() -> Self {
        Self {
            date_formats: vec![
                "%Y-%m-%d".to_string(), // ISO format: 2023-01-15
                "%d-%m-%Y".to_string(), // European: 15-01-2023
                "%m/%d/%Y".to_string(), // US: 01/15/2023
                "%d/%m/%Y".to_string(), // UK: 15/01/2023
                "%d.%m.%Y".to_string(), // German/Danish: 15.01.2023
                "%Y%m%d".to_string(),   // Compact: 20230115
                "%d %b %Y".to_string(), // 15 Jan 2023
                "%d %B %Y".to_string(), // 15 January 2023
            ],
            default_format: "%Y-%m-%d".to_string(),
            enable_format_detection: true,
        }
    }
}

/// Parse a date string with multiple format attempts
#[must_use]
pub fn parse_date_string(s: &str, config: &DateFormatConfig) -> Option<NaiveDate> {
    // Try all the provided formats
    for format in &config.date_formats {
        if let Ok(date) = NaiveDate::parse_from_str(s, format) {
            return Some(date);
        }
    }

    // If enabled, try to detect the format based on string patterns
    if config.enable_format_detection {
        if let Some(detected_format) = detect_date_format(s) {
            if let Ok(date) = NaiveDate::parse_from_str(s, &detected_format) {
                return Some(date);
            }
        }
    }

    None
}

/// Try to detect the date format based on string patterns
fn detect_date_format(s: &str) -> Option<String> {
    // Check for ISO-like format with dashes (YYYY-MM-DD)
    if s.len() == 10 && s.chars().nth(4) == Some('-') && s.chars().nth(7) == Some('-') {
        return Some("%Y-%m-%d".to_string());
    }

    // Check for slashes
    if s.contains('/') {
        let parts: Vec<&str> = s.split('/').collect();
        if parts.len() == 3 {
            if parts[0].len() == 4 {
                return Some("%Y/%m/%d".to_string()); // YYYY/MM/DD
            } else if parts[2].len() == 4 {
                // Check if first part is likely day or month
                if let Ok(first_num) = parts[0].parse::<u8>() {
                    if first_num > 12 {
                        return Some("%d/%m/%Y".to_string()); // DD/MM/YYYY
                    }
                    // Could be either MM/DD/YYYY or DD/MM/YYYY
                    // Default to European format, but this might need context-specific logic
                    return Some("%d/%m/%Y".to_string());
                }
            }
        }
    }

    // Check for dots (DD.MM.YYYY)
    if s.contains('.') {
        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() == 3 && parts[2].len() == 4 {
            return Some("%d.%m.%Y".to_string());
        }
    }

    // Check for compact format (YYYYMMDD)
    if s.len() == 8 && s.chars().all(|c| c.is_ascii_digit()) {
        return Some("%Y%m%d".to_string());
    }

    // No recognized format
    None
}

// Specific array conversion implementations:

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

        // Convert to debug string format
        let value = format!("{array:?}");
        string_builder.append_value(&value);
    }

    Ok(Arc::new(string_builder.finish()) as ArrayRef)
}
