//! Field mapping system using registry traits
//!
//! This module provides a type-safe field mapping system that uses the registry-specific
//! trait methods to set field values. It enables a clear separation between registry
//! field definitions and their mapping to the domain model.

use crate::models::core::registry_traits::*;
use crate::schema::{FieldDefinition, FieldType};
use arrow::array::{ArrayRef, StringArray, Int32Array, Float64Array, Date32Array};
use arrow::datatypes::DataType;
use chrono::{NaiveDate, Days};
use std::sync::Arc;

/// Type-safe field mapping
pub struct FieldMapping<T> {
    /// Field definition containing metadata
    pub field_def: FieldDefinition,
    /// Extraction function that gets a value from a RecordBatch
    pub extractor: Box<dyn Fn(&ArrayRef, usize) -> Option<T> + Send + Sync>,
    /// Setter function that applies the value to an Individual object
    pub setter: Box<dyn Fn(&mut dyn Any, T) -> () + Send + Sync>,
}

/// Helper trait to allow dynamic dispatch on setter functions
pub trait Any: BefFields + LprFields + MfrFields + UddfFields + IndFields + AkmFields + VndsFields + DodFields {}

// Implement Any for Individual
impl Any for crate::models::core::Individual {}

impl<T> FieldMapping<T> {
    /// Create a new field mapping with the given components
    pub fn new(
        field_def: FieldDefinition,
        extractor: Box<dyn Fn(&ArrayRef, usize) -> Option<T> + Send + Sync>,
        setter: Box<dyn Fn(&mut dyn Any, T) -> () + Send + Sync>,
    ) -> Self {
        Self {
            field_def,
            extractor,
            setter,
        }
    }
    
    /// Apply the mapping to an individual object using data from an array
    pub fn apply(&self, individual: &mut dyn Any, array: &ArrayRef, row: usize) {
        if let Some(value) = (self.extractor)(array, row) {
            (self.setter)(individual, value);
        }
    }
}

/// Factory methods for extracting values from Arrow arrays
pub struct Extractors;

impl Extractors {
    /// Extract a string value from a field in a record batch
    pub fn string(field_name: &'static str) -> Box<dyn Fn(&ArrayRef, usize) -> Option<String> + Send + Sync> {
        Box::new(move |array, row| {
            let string_array = array.as_any().downcast_ref::<StringArray>()?;
            if row < string_array.len() && !string_array.is_null(row) {
                Some(string_array.value(row).to_string())
            } else {
                None
            }
        })
    }
    
    /// Extract an integer value from a field in a record batch
    pub fn integer(field_name: &'static str) -> Box<dyn Fn(&ArrayRef, usize) -> Option<i32> + Send + Sync> {
        Box::new(move |array, row| {
            let int_array = array.as_any().downcast_ref::<Int32Array>()?;
            if row < int_array.len() && !int_array.is_null(row) {
                Some(int_array.value(row))
            } else {
                None
            }
        })
    }
    
    /// Extract a float value from a field in a record batch
    pub fn float(field_name: &'static str) -> Box<dyn Fn(&ArrayRef, usize) -> Option<f64> + Send + Sync> {
        Box::new(move |array, row| {
            let float_array = array.as_any().downcast_ref::<Float64Array>()?;
            if row < float_array.len() && !float_array.is_null(row) {
                Some(float_array.value(row))
            } else {
                None
            }
        })
    }
    
    /// Extract a date value from a field in a record batch
    pub fn date(field_name: &'static str) -> Box<dyn Fn(&ArrayRef, usize) -> Option<NaiveDate> + Send + Sync> {
        Box::new(move |array, row| {
            let date_array = array.as_any().downcast_ref::<Date32Array>()?;
            if row < date_array.len() && !date_array.is_null(row) {
                let days_since_epoch = date_array.value(row);
                Some(
                    NaiveDate::from_ymd_opt(1970, 1, 1)?
                        .checked_add_days(Days::new(days_since_epoch as u64))?,
                )
            } else {
                None
            }
        })
    }
}

/// Factory methods for type-safe field setters
pub struct FieldSetters;

impl FieldSetters {
    // BEF Registry setter factories
    
    /// Create a setter for spouse_pnr field (BEF)
    pub fn spouse_pnr() -> Box<dyn Fn(&mut dyn Any, String) -> () + Send + Sync> {
        Box::new(|individual, value| {
            individual.set_spouse_pnr(Some(value));
        })
    }
    
    /// Create a setter for family_size field (BEF)
    pub fn family_size() -> Box<dyn Fn(&mut dyn Any, i32) -> () + Send + Sync> {
        Box::new(|individual, value| {
            individual.set_family_size(Some(value));
        })
    }
    
    /// Create a setter for residence_from field (BEF)
    pub fn residence_from() -> Box<dyn Fn(&mut dyn Any, NaiveDate) -> () + Send + Sync> {
        Box::new(|individual, value| {
            individual.set_residence_from(Some(value));
        })
    }
    
    // LPR Registry setter factories
    
    /// Create a setter to add a diagnosis (LPR)
    pub fn add_diagnosis() -> Box<dyn Fn(&mut dyn Any, String) -> () + Send + Sync> {
        Box::new(|individual, value| {
            individual.add_diagnosis(value);
        })
    }
    
    /// Create a setter to add a procedure (LPR)
    pub fn add_procedure() -> Box<dyn Fn(&mut dyn Any, String) -> () + Send + Sync> {
        Box::new(|individual, value| {
            individual.add_procedure(value);
        })
    }
    
    /// Create a setter for length_of_stay field (LPR)
    pub fn length_of_stay() -> Box<dyn Fn(&mut dyn Any, i32) -> () + Send + Sync> {
        Box::new(|individual, value| {
            individual.set_length_of_stay(Some(value));
        })
    }
    
    // MFR Registry setter factories
    
    /// Create a setter for birth_weight field (MFR)
    pub fn birth_weight() -> Box<dyn Fn(&mut dyn Any, i32) -> () + Send + Sync> {
        Box::new(|individual, value| {
            individual.set_birth_weight(Some(value));
        })
    }
    
    /// Create a setter for birth_length field (MFR)
    pub fn birth_length() -> Box<dyn Fn(&mut dyn Any, i32) -> () + Send + Sync> {
        Box::new(|individual, value| {
            individual.set_birth_length(Some(value));
        })
    }
    
    // More factory methods for other registry fields...
}

/// Create a mapping for a field in the BEF registry
pub fn create_bef_field_mapping(
    field_name: &str,
    description: &str,
    field_type: FieldType,
    nullable: bool,
    arrow_field_name: &'static str,
) -> FieldMapping<String> {
    let field_def = FieldDefinition::new(field_name, description, field_type, nullable);
    let extractor = Extractors::string(arrow_field_name);
    
    // Map field name to the appropriate setter
    let setter = match field_name {
        "AEGTE_ID" => FieldSetters::spouse_pnr(),
        // Add mappings for other BEF fields...
        _ => {
            // Default handler for unmapped fields
            Box::new(move |_individual: &mut dyn Any, _value: String| {
                // Do nothing for unmapped fields
            })
        }
    };
    
    FieldMapping::new(field_def, extractor, setter)
}