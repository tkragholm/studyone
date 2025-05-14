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

/// Re-export `ModelSetter` trait and the associated type
pub use crate::schema::field_def::mapping::ModelSetter;

/// Wrapper struct for setter functions to allow Debug implementation
#[derive(Clone)]
pub struct Setter(pub Arc<dyn ModelSetter>);

impl Setter {
    /// Create a new setter from a `ModelSetter`
    pub fn new(setter: Arc<dyn ModelSetter>) -> Self {
        Self(setter)
    }

    /// Call the underlying function
    pub fn call(&self, target: &mut dyn Any, value: Box<dyn Any>) {
        (self.0)(target, value);
    }
}

/// Debug implementation for Setter
impl std::fmt::Debug for Setter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Setter{{...}}")
    }
}

/// Generic field extractor for string fields
#[derive(Debug)]
pub struct StringExtractor {
    source_field: String,
    target_field: String,
    #[allow(missing_debug_implementations)]
    setter: Setter,
}

impl StringExtractor {
    /// Create a new string field extractor
    #[must_use] pub fn new(source_field: &str, target_field: &str, setter: Setter) -> Self {
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
                    self.setter.call(target, Box::new(string_value));
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
#[derive(Debug)]
pub struct IntegerExtractor {
    source_field: String,
    target_field: String,
    #[allow(missing_debug_implementations)]
    setter: Setter,
}

impl IntegerExtractor {
    /// Create a new integer field extractor
    #[must_use] pub fn new(source_field: &str, target_field: &str, setter: Setter) -> Self {
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
                    self.setter.call(target, Box::new(int_value));
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
#[derive(Debug)]
pub struct FloatExtractor {
    source_field: String,
    target_field: String,
    #[allow(missing_debug_implementations)]
    setter: Setter,
}

impl FloatExtractor {
    /// Create a new float field extractor
    #[must_use] pub fn new(source_field: &str, target_field: &str, setter: Setter) -> Self {
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
                    self.setter.call(target, Box::new(float_value));
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
#[derive(Debug)]
pub struct DateExtractor {
    source_field: String,
    target_field: String,
    #[allow(missing_debug_implementations)]
    setter: Setter,
}

impl DateExtractor {
    /// Create a new date field extractor
    #[must_use] pub fn new(source_field: &str, target_field: &str, setter: Setter) -> Self {
        Self {
            source_field: source_field.to_string(),
            target_field: target_field.to_string(),
            setter,
        }
    }

    /// Parse date from string in various formats
    fn parse_date(&self, date_str: &str) -> Option<NaiveDate> {
        // Try various date formats
        
        // 1. ISO format (YYYY-MM-DD)
        if let Ok(date) = chrono::NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
            return Some(date);
        }
        
        // 2. DD/MM/YYYY format
        if let Ok(date) = chrono::NaiveDate::parse_from_str(date_str, "%d/%m/%Y") {
            return Some(date);
        }
        
        // 3. YYYYMMDD format
        if date_str.len() == 8 && date_str.chars().all(|c| c.is_ascii_digit()) {
            if let (Ok(year), Ok(month), Ok(day)) = (
                date_str[0..4].parse::<i32>(),
                date_str[4..6].parse::<u32>(),
                date_str[6..8].parse::<u32>(),
            ) {
                return chrono::NaiveDate::from_ymd_opt(year, month, day);
            }
        }
        
        // 4. %d%b%Y format (e.g., "01Jan2020")
        if let Ok(date) = chrono::NaiveDate::parse_from_str(date_str, "%d%b%Y") {
            return Some(date);
        }
        
        // 5. Try case insensitive month names
        let date_str_lower = date_str.to_lowercase();
        let months = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"];
        
        if date_str_lower.len() >= 5 {
            // Look for a month name in the string
            for (i, &month) in months.iter().enumerate() {
                if date_str_lower.contains(month) {
                    // Try to extract day and year around the month
                    let parts: Vec<&str> = date_str_lower.split(month).collect();
                    if parts.len() == 2 {
                        let day_part = parts[0].trim();
                        let year_part = parts[1].trim();
                        
                        if let (Ok(day), Ok(year)) = (day_part.parse::<u32>(), year_part.parse::<i32>()) {
                            let month_num = i as u32 + 1;
                            return chrono::NaiveDate::from_ymd_opt(year, month_num, day);
                        }
                    }
                }
            }
        }
        
        // For debugging, but limit output
        static mut FAILURE_COUNT: usize = 0;
        unsafe {
            if FAILURE_COUNT < 3 {
                println!("Failed to parse date string: '{date_str}'");
                FAILURE_COUNT += 1;
            }
        }
        None
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
                        // Add debug logging for the first few date values
                        static mut DEBUG_COUNT: usize = 0;
                        unsafe {
                            if DEBUG_COUNT < 5 {
                                println!("Parsing date string: '{}' for field '{}'", 
                                         string_array.value(row), self.source_field);
                                DEBUG_COUNT += 1;
                            }
                        }
                        
                        let date = self.parse_date(string_array.value(row));
                        // Add debug logging for the first few successful date parses
                        static mut SUCCESS_COUNT: usize = 0;
                        if date.is_some() {
                            unsafe {
                                if SUCCESS_COUNT < 3 {
                                    println!("Successfully parsed date: {date:?}");
                                    SUCCESS_COUNT += 1;
                                }
                            }
                        }
                        date
                    } else {
                        None
                    }
                } else {
                    None
                };

                // Set the value using the provided setter
                // For date fields, we need to box it as an Option<NaiveDate> since
                // that's what the Individual struct expects
                match value {
                    Some(date) => {
                        // We need to convert Option<NaiveDate> to NaiveDate then box it to send to setter
                        // The setter will handle wrapping it back in an Option before storing
                        self.setter.call(target, Box::new(date));
                    }
                    None => {
                        // For None values, pass a special marker
                        self.setter.call(target, Box::new("__DATE_NONE__"));
                    }
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
