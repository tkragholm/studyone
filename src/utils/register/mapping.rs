//! Trait-based field mapping system
//!
//! This module provides a field mapping system that uses registry-specific
//! trait methods for setting field values, creating a type-safe interface
//! for mapping registry fields to the Individual model.

use std::any::Any;

use arrow::array::{Array, ArrayRef, Date32Array, StringArray};
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;

use crate::error::Result;
use crate::models::core::Individual;
use crate::models::core::registry_traits::{AkmFields, BefFields, DodFields, IndFields, LprFields, MfrFields, UddfFields, VndsFields};

/// Field definition with metadata
#[derive(Debug, Clone)]
pub struct FieldDefinition {
    /// Field name in the registry data
    pub name: String,
    /// Field description
    pub description: String,
    /// Required or optional field
    pub required: bool,
    /// Field type identifier
    pub field_type: FieldType,
}

/// Field type identifiers
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldType {
    /// String field type
    String,

    /// Integer field type
    Integer,

    /// Float field type
    Float,

    /// Boolean field type
    Boolean,

    /// Date field type
    Date,

    /// Personal identification number (PNR)
    PNR,

    /// Array of strings
    StringArray,

    /// Array of dates
    DateArray,
}

impl FieldDefinition {
    /// Create a new field definition
    #[must_use] pub fn new(name: &str, description: &str, field_type: FieldType, required: bool) -> Self {
        Self {
            name: name.to_string(),
            description: description.to_string(),
            required,
            field_type,
        }
    }
}

/// Trait-based field mapping
///
/// This struct defines a mapping between a registry field and an Individual
/// model field, using trait-based field access methods.
pub struct FieldMapping<T: 'static> {
    /// Field definition with metadata
    pub field_def: FieldDefinition,

    /// Arrow array value extractor function
    pub extractor: Box<dyn Fn(&ArrayRef, usize) -> Option<T> + Send + Sync>,

    /// Trait-based field setter function
    pub setter: Box<dyn Fn(&mut dyn Any, T) + Send + Sync>,
}

impl<T: 'static> FieldMapping<T> {
    /// Create a new field mapping
    pub fn new<F, S>(field_def: FieldDefinition, extractor: F, setter: S) -> Self
    where
        F: Fn(&ArrayRef, usize) -> Option<T> + Send + Sync + 'static,
        S: Fn(&mut dyn Any, T) + Send + Sync + 'static,
    {
        Self {
            field_def,
            extractor: Box::new(extractor),
            setter: Box::new(setter),
        }
    }

    /// Apply this mapping to an individual
    ///
    /// # Arguments
    ///
    /// * `individual` - The individual to apply the mapping to
    /// * `array` - The array to extract the value from
    /// * `row` - The row index to extract
    ///
    /// This method extracts a value from the array and sets it on the individual
    /// using the trait-based setter method.
    pub fn apply(&self, individual: &mut dyn Any, array: &ArrayRef, row: usize) -> Result<()> {
        if let Some(value) = (self.extractor)(array, row) {
            (self.setter)(individual, value);
        }
        Ok(())
    }
}

/// Create a string field mapping
///
/// # Arguments
///
/// * `name` - Field name in the registry data
/// * `description` - Field description
/// * `required` - Whether the field is required
/// * `setter` - Trait-based field setter function
pub fn string_field<F>(
    name: &str,
    description: &str,
    required: bool,
    setter: F,
) -> FieldMapping<String>
where
    F: Fn(&mut dyn Any, String) + Send + Sync + 'static,
{
    FieldMapping::new(
        FieldDefinition::new(name, description, FieldType::String, required),
        |array, idx| {
            if let Some(string_array) = array.as_any().downcast_ref::<StringArray>() {
                if !string_array.is_null(idx) {
                    return Some(string_array.value(idx).to_string());
                }
            }
            None
        },
        setter,
    )
}

/// Create a date field mapping
///
/// # Arguments
///
/// * `name` - Field name in the registry data
/// * `description` - Field description
/// * `required` - Whether the field is required
/// * `setter` - Trait-based field setter function
pub fn date_field<F>(
    name: &str,
    description: &str,
    required: bool,
    setter: F,
) -> FieldMapping<NaiveDate>
where
    F: Fn(&mut dyn Any, NaiveDate) + Send + Sync + 'static,
{
    FieldMapping::new(
        FieldDefinition::new(name, description, FieldType::Date, required),
        |array, idx| {
            if let Some(date_array) = array.as_any().downcast_ref::<Date32Array>() {
                if !date_array.is_null(idx) {
                    let days_since_epoch = date_array.value(idx);
                    // Convert from days since UNIX epoch to NaiveDate
                    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                    return Some(epoch + chrono::Duration::days(i64::from(days_since_epoch)));
                }
            }
            None
        },
        setter,
    )
}

/// Create an integer field mapping
///
/// # Arguments
///
/// * `name` - Field name in the registry data
/// * `description` - Field description
/// * `required` - Whether the field is required
/// * `setter` - Trait-based field setter function
pub fn integer_field<F>(
    name: &str,
    description: &str,
    required: bool,
    setter: F,
) -> FieldMapping<i32>
where
    F: Fn(&mut dyn Any, i32) + Send + Sync + 'static,
{
    FieldMapping::new(
        FieldDefinition::new(name, description, FieldType::Integer, required),
        |array, idx| {
            if array.is_null(idx) {
                return None;
            }

            // Extract the value based on data type
            match array.data_type() {
                arrow::datatypes::DataType::Int8 => {
                    let array =
                        arrow::array::cast::as_primitive_array::<arrow::datatypes::Int8Type>(array);
                    Some(i32::from(array.value(idx)))
                }
                arrow::datatypes::DataType::Int16 => {
                    let array = arrow::array::cast::as_primitive_array::<arrow::datatypes::Int16Type>(
                        array,
                    );
                    Some(i32::from(array.value(idx)))
                }
                arrow::datatypes::DataType::Int32 => {
                    let array = arrow::array::cast::as_primitive_array::<arrow::datatypes::Int32Type>(
                        array,
                    );
                    Some(array.value(idx))
                }
                arrow::datatypes::DataType::Int64 => {
                    let array = arrow::array::cast::as_primitive_array::<arrow::datatypes::Int64Type>(
                        array,
                    );
                    Some(array.value(idx) as i32)
                }
                arrow::datatypes::DataType::UInt8 => {
                    let array = arrow::array::cast::as_primitive_array::<arrow::datatypes::UInt8Type>(
                        array,
                    );
                    Some(i32::from(array.value(idx)))
                }
                arrow::datatypes::DataType::UInt16 => {
                    let array = arrow::array::cast::as_primitive_array::<
                        arrow::datatypes::UInt16Type,
                    >(array);
                    Some(i32::from(array.value(idx)))
                }
                arrow::datatypes::DataType::UInt32 => {
                    let array = arrow::array::cast::as_primitive_array::<
                        arrow::datatypes::UInt32Type,
                    >(array);
                    Some(array.value(idx) as i32)
                }
                _ => None,
            }
        },
        setter,
    )
}

/// Create a float field mapping
///
/// # Arguments
///
/// * `name` - Field name in the registry data
/// * `description` - Field description
/// * `required` - Whether the field is required
/// * `setter` - Trait-based field setter function
pub fn float_field<F>(name: &str, description: &str, required: bool, setter: F) -> FieldMapping<f64>
where
    F: Fn(&mut dyn Any, f64) + Send + Sync + 'static,
{
    FieldMapping::new(
        FieldDefinition::new(name, description, FieldType::Float, required),
        |array, idx| {
            if array.is_null(idx) {
                return None;
            }

            // Extract the value based on data type
            match array.data_type() {
                arrow::datatypes::DataType::Float32 => {
                    let array = arrow::array::cast::as_primitive_array::<
                        arrow::datatypes::Float32Type,
                    >(array);
                    Some(f64::from(array.value(idx)))
                }
                arrow::datatypes::DataType::Float64 => {
                    let array = arrow::array::cast::as_primitive_array::<
                        arrow::datatypes::Float64Type,
                    >(array);
                    Some(array.value(idx))
                }
                _ => None,
            }
        },
        setter,
    )
}

/// Registry field mapper
///
/// This trait defines an interface for mapping registry fields to Individual
/// model fields using trait-based field access methods.
pub trait RegistryFieldMapper: Send + Sync {
    /// Get the registry type name
    fn registry_type(&self) -> &str;

    /// Apply all field mappings to an individual
    ///
    /// # Arguments
    ///
    /// * `individual` - The individual to apply mappings to
    /// * `record_batch` - The record batch to extract values from
    /// * `row` - The row index to extract
    ///
    /// # Returns
    ///
    /// A Result indicating success or failure
    fn apply_mappings(
        &self,
        individual: &mut dyn Any,
        record_batch: &RecordBatch,
        row: usize,
    ) -> Result<()>;
}

/// Helper function to cast Any to `BefFields`
pub fn as_bef_fields(any: &mut dyn Any) -> Option<&mut dyn BefFields> {
    if let Some(individual) = any.downcast_mut::<Individual>() {
        Some(individual as &mut dyn BefFields)
    } else {
        None
    }
}

/// Helper function to cast Any to `LprFields`
pub fn as_lpr_fields(any: &mut dyn Any) -> Option<&mut dyn LprFields> {
    if let Some(individual) = any.downcast_mut::<Individual>() {
        Some(individual as &mut dyn LprFields)
    } else {
        None
    }
}

/// Helper function to cast Any to `MfrFields`
pub fn as_mfr_fields(any: &mut dyn Any) -> Option<&mut dyn MfrFields> {
    if let Some(individual) = any.downcast_mut::<Individual>() {
        Some(individual as &mut dyn MfrFields)
    } else {
        None
    }
}

/// Helper function to cast Any to `UddfFields`
#[allow(dead_code)]
pub fn as_uddf_fields(any: &mut dyn Any) -> Option<&mut dyn UddfFields> {
    if let Some(individual) = any.downcast_mut::<Individual>() {
        Some(individual as &mut dyn UddfFields)
    } else {
        None
    }
}

/// Helper function to cast Any to `IndFields`
#[allow(dead_code)]
pub fn as_ind_fields(any: &mut dyn Any) -> Option<&mut dyn IndFields> {
    if let Some(individual) = any.downcast_mut::<Individual>() {
        Some(individual as &mut dyn IndFields)
    } else {
        None
    }
}

/// Helper function to cast Any to `AkmFields`
pub fn as_akm_fields(any: &mut dyn Any) -> Option<&mut dyn AkmFields> {
    if let Some(individual) = any.downcast_mut::<Individual>() {
        Some(individual as &mut dyn AkmFields)
    } else {
        None
    }
}

/// Helper function to cast Any to `VndsFields`
pub fn as_vnds_fields(any: &mut dyn Any) -> Option<&mut dyn VndsFields> {
    if let Some(individual) = any.downcast_mut::<Individual>() {
        Some(individual as &mut dyn VndsFields)
    } else {
        None
    }
}

/// Helper function to cast Any to `DodFields`
pub fn as_dod_fields(any: &mut dyn Any) -> Option<&mut dyn DodFields> {
    if let Some(individual) = any.downcast_mut::<Individual>() {
        Some(individual as &mut dyn DodFields)
    } else {
        None
    }
}
