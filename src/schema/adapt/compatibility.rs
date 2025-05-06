//! Module for handling data type compatibility checks.

use arrow::datatypes::DataType;
use crate::schema::adapt::types::{TypeCompatibility, AdaptationStrategy};

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

/// Determine the appropriate adaptation strategy for a given source and target type
#[must_use]
pub const fn determine_adaptation_strategy(
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