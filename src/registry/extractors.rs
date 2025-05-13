//! Common field extractors for registry deserializers
//!
//! This module provides reusable field extractors for different data types
//! to eliminate code duplication across registry deserializers.

use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, Date32Array, Float64Array, StringArray};
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;

use crate::error::Result;
use crate::registry::trait_deserializer::RegistryFieldExtractor;

/// Type alias for field setter closures
pub type Setter = Arc<dyn Fn(&mut dyn Any, Box<dyn Any>) + Send + Sync>;

/// Generic field extractor for string fields
pub struct StringExtractor {
    source_field: String,
    target_field: String,
    setter: Setter,
}

impl StringExtractor {
    /// Create a new string field extractor
    pub fn new(source_field: &str, target_field: &str, setter: Setter) -> Self {
        Self {
            source_field: source_field.to_string(),
            target_field: target_field.to_string(),
            setter,
        }
    }
}

impl RegistryFieldExtractor for StringExtractor {
    fn extract_and_set(&self, batch: &RecordBatch, row: usize, target: &mut dyn Any) -> Result<()> {
        // Get the column by name
        match batch.column_by_name(&self.source_field) {
            Some(array) => {
                // Extract the value as a string
                let value = if array.is_null(row) {
                    None
                } else if let Some(string_array) = array.as_any().downcast_ref::<StringArray>() {
                    Some(string_array.value(row).to_string())
                } else {
                    // Try to convert any other type to string
                    Some(format!("{array:?}"))
                };

                // Set the value using the provided setter
                if let Some(string_value) = value {
                    (self.setter)(target, Box::new(string_value));
                }
                Ok(())
            }
            None => {
                // Field not found, just skip it
                Ok(())
            }
        }
    }

    fn source_field_name(&self) -> &str {
        &self.source_field
    }

    fn target_field_name(&self) -> &str {
        &self.target_field
    }
}

/// Generic field extractor for integer fields
pub struct IntegerExtractor {
    source_field: String,
    target_field: String,
    setter: Setter,
}

impl IntegerExtractor {
    /// Create a new integer field extractor
    pub fn new(source_field: &str, target_field: &str, setter: Setter) -> Self {
        Self {
            source_field: source_field.to_string(),
            target_field: target_field.to_string(),
            setter,
        }
    }
}

impl RegistryFieldExtractor for IntegerExtractor {
    fn extract_and_set(&self, batch: &RecordBatch, row: usize, target: &mut dyn Any) -> Result<()> {
        // Get the column by name
        match batch.column_by_name(&self.source_field) {
            Some(array) => {
                // Extract the value as an integer
                let value = if array.is_null(row) {
                    None
                } else {
                    // We use a generic approach that works with various numeric array types
                    match array.data_type() {
                        arrow::datatypes::DataType::Int8 => {
                            let array = arrow::array::cast::as_primitive_array::<
                                arrow::datatypes::Int8Type,
                            >(array);
                            Some(i32::from(array.value(row)))
                        }
                        arrow::datatypes::DataType::Int16 => {
                            let array = arrow::array::cast::as_primitive_array::<
                                arrow::datatypes::Int16Type,
                            >(array);
                            Some(i32::from(array.value(row)))
                        }
                        arrow::datatypes::DataType::Int32 => {
                            let array = arrow::array::cast::as_primitive_array::<
                                arrow::datatypes::Int32Type,
                            >(array);
                            Some(array.value(row))
                        }
                        arrow::datatypes::DataType::Int64 => {
                            let array = arrow::array::cast::as_primitive_array::<
                                arrow::datatypes::Int64Type,
                            >(array);
                            Some(array.value(row) as i32)
                        }
                        arrow::datatypes::DataType::UInt8 => {
                            let array = arrow::array::cast::as_primitive_array::<
                                arrow::datatypes::UInt8Type,
                            >(array);
                            Some(i32::from(array.value(row)))
                        }
                        arrow::datatypes::DataType::UInt16 => {
                            let array = arrow::array::cast::as_primitive_array::<
                                arrow::datatypes::UInt16Type,
                            >(array);
                            Some(i32::from(array.value(row)))
                        }
                        arrow::datatypes::DataType::UInt32 => {
                            let array = arrow::array::cast::as_primitive_array::<
                                arrow::datatypes::UInt32Type,
                            >(array);
                            Some(array.value(row) as i32)
                        }
                        _ => None,
                    }
                };

                // Set the value using the provided setter
                if let Some(int_value) = value {
                    (self.setter)(target, Box::new(int_value));
                }
                Ok(())
            }
            None => {
                // Field not found, just skip it
                Ok(())
            }
        }
    }

    fn source_field_name(&self) -> &str {
        &self.source_field
    }

    fn target_field_name(&self) -> &str {
        &self.target_field
    }
}

/// Generic field extractor for float fields
pub struct FloatExtractor {
    source_field: String,
    target_field: String,
    setter: Setter,
}

impl FloatExtractor {
    /// Create a new float field extractor
    pub fn new(source_field: &str, target_field: &str, setter: Setter) -> Self {
        Self {
            source_field: source_field.to_string(),
            target_field: target_field.to_string(),
            setter,
        }
    }
}

impl RegistryFieldExtractor for FloatExtractor {
    fn extract_and_set(&self, batch: &RecordBatch, row: usize, target: &mut dyn Any) -> Result<()> {
        // Get the column by name
        match batch.column_by_name(&self.source_field) {
            Some(array) => {
                // Extract the value as a float
                let value = if array.is_null(row) {
                    None
                } else if let Some(float_array) = array.as_any().downcast_ref::<Float64Array>() {
                    Some(float_array.value(row))
                } else {
                    // Try to convert any other numeric array to float
                    match array.data_type() {
                        arrow::datatypes::DataType::Float32 => {
                            let array = arrow::array::cast::as_primitive_array::<
                                arrow::datatypes::Float32Type,
                            >(array);
                            Some(f64::from(array.value(row)))
                        }
                        arrow::datatypes::DataType::Int8 => {
                            let array = arrow::array::cast::as_primitive_array::<
                                arrow::datatypes::Int8Type,
                            >(array);
                            Some(f64::from(array.value(row)))
                        }
                        arrow::datatypes::DataType::Int16 => {
                            let array = arrow::array::cast::as_primitive_array::<
                                arrow::datatypes::Int16Type,
                            >(array);
                            Some(f64::from(array.value(row)))
                        }
                        arrow::datatypes::DataType::Int32 => {
                            let array = arrow::array::cast::as_primitive_array::<
                                arrow::datatypes::Int32Type,
                            >(array);
                            Some(f64::from(array.value(row)))
                        }
                        arrow::datatypes::DataType::Int64 => {
                            let array = arrow::array::cast::as_primitive_array::<
                                arrow::datatypes::Int64Type,
                            >(array);
                            Some(array.value(row) as f64)
                        }
                        _ => None,
                    }
                };

                // Set the value using the provided setter
                if let Some(float_value) = value {
                    (self.setter)(target, Box::new(float_value));
                }
                Ok(())
            }
            None => {
                // Field not found, just skip it
                Ok(())
            }
        }
    }

    fn source_field_name(&self) -> &str {
        &self.source_field
    }

    fn target_field_name(&self) -> &str {
        &self.target_field
    }
}

/// Generic field extractor for date fields
pub struct DateExtractor {
    source_field: String,
    target_field: String,
    setter: Setter,
}

impl DateExtractor {
    /// Create a new date field extractor
    pub fn new(source_field: &str, target_field: &str, setter: Setter) -> Self {
        Self {
            source_field: source_field.to_string(),
            target_field: target_field.to_string(),
            setter,
        }
    }

    /// Parse date from string in YYYYMMDD format
    fn parse_date(&self, date_str: &str) -> Option<NaiveDate> {
        if date_str.len() == 8 {
            let year = date_str[0..4].parse::<i32>().ok()?;
            let month = date_str[4..6].parse::<u32>().ok()?;
            let day = date_str[6..8].parse::<u32>().ok()?;
            chrono::NaiveDate::from_ymd_opt(year, month, day)
        } else {
            None
        }
    }
}

impl RegistryFieldExtractor for DateExtractor {
    fn extract_and_set(&self, batch: &RecordBatch, row: usize, target: &mut dyn Any) -> Result<()> {
        // Get the column by name
        match batch.column_by_name(&self.source_field) {
            Some(array) => {
                // Extract the value as a date
                let value = if array.is_null(row) {
                    None
                } else if let Some(date_array) = array.as_any().downcast_ref::<Date32Array>() {
                    let days_since_epoch = date_array.value(row);
                    // Convert from days since UNIX epoch to NaiveDate
                    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                    Some(epoch + chrono::Duration::days(i64::from(days_since_epoch)))
                } else if let Some(string_array) = array.as_any().downcast_ref::<StringArray>() {
                    // Try to parse date from string (often used in string dates)
                    if row < string_array.len() && !string_array.is_null(row) {
                        self.parse_date(string_array.value(row))
                    } else {
                        None
                    }
                } else {
                    None
                };

                // Set the value using the provided setter
                if let Some(date_value) = value {
                    (self.setter)(target, Box::new(date_value));
                }
                Ok(())
            }
            None => {
                // Field not found, just skip it
                Ok(())
            }
        }
    }

    fn source_field_name(&self) -> &str {
        &self.source_field
    }

    fn target_field_name(&self) -> &str {
        &self.target_field
    }
}
