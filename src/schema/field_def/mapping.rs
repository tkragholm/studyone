//! Field mapping for the unified schema system
//! 
//! This module defines mappings between registry fields and the Individual model.

use std::sync::Arc;
use arrow::record_batch::RecordBatch;
use arrow::array::{
    StringArray, Int32Array, Float64Array, BooleanArray, Date32Array, Array,
};
use crate::models::core::Individual;
use super::field::{FieldDefinition, FieldType};

/// A trait for functions that set values on the Individual model
pub trait ModelSetter: Fn(&mut Individual, Box<dyn std::any::Any>) + Send + Sync + 'static {}

// Implement the trait for all compatible function types
impl<F> ModelSetter for F where F: Fn(&mut Individual, Box<dyn std::any::Any>) + Send + Sync + 'static {}

/// Type-safe model setter functions
pub struct ModelSetters;

impl ModelSetters {
    // Helper function to create model setters
    pub fn string_setter<F>(setter_fn: F) -> Arc<dyn ModelSetter>
    where
        F: Fn(&mut Individual, String) + Send + Sync + 'static,
    {
        Arc::new(move |individual: &mut Individual, value: Box<dyn std::any::Any>| {
            if let Ok(string_value) = value.downcast::<String>() {
                setter_fn(individual, *string_value);
            }
        })
    }

    pub fn i32_setter<F>(setter_fn: F) -> Arc<dyn ModelSetter>
    where
        F: Fn(&mut Individual, i32) + Send + Sync + 'static,
    {
        Arc::new(move |individual: &mut Individual, value: Box<dyn std::any::Any>| {
            if let Ok(int_value) = value.downcast::<i32>() {
                setter_fn(individual, *int_value);
            }
        })
    }

    pub fn f64_setter<F>(setter_fn: F) -> Arc<dyn ModelSetter>
    where
        F: Fn(&mut Individual, f64) + Send + Sync + 'static,
    {
        Arc::new(move |individual: &mut Individual, value: Box<dyn std::any::Any>| {
            if let Ok(float_value) = value.downcast::<f64>() {
                setter_fn(individual, *float_value);
            }
        })
    }

    pub fn bool_setter<F>(setter_fn: F) -> Arc<dyn ModelSetter>
    where
        F: Fn(&mut Individual, bool) + Send + Sync + 'static,
    {
        Arc::new(move |individual: &mut Individual, value: Box<dyn std::any::Any>| {
            if let Ok(bool_value) = value.downcast::<bool>() {
                setter_fn(individual, *bool_value);
            }
        })
    }

    pub fn date_setter<F>(setter_fn: F) -> Arc<dyn ModelSetter>
    where
        F: Fn(&mut Individual, chrono::NaiveDate) + Send + Sync + 'static,
    {
        Arc::new(move |individual: &mut Individual, value: Box<dyn std::any::Any>| {
            if let Ok(date_value) = value.downcast::<chrono::NaiveDate>() {
                setter_fn(individual, *date_value);
            }
        })
    }
}

/// A mapping between a registry field and an Individual model field
#[derive(Clone)]
pub struct FieldMapping {
    /// The field definition
    pub field_def: FieldDefinition,
    /// Function to extract a value from a record batch
    pub extractor: Arc<dyn Fn(&RecordBatch, usize) -> Option<Box<dyn std::any::Any>> + Send + Sync>,
    /// Function to set the value on an Individual model
    pub setter: Arc<dyn ModelSetter>,
}

impl FieldMapping {
    /// Create a new field mapping
    pub fn new(
        field_def: FieldDefinition,
        extractor: Arc<dyn Fn(&RecordBatch, usize) -> Option<Box<dyn std::any::Any>> + Send + Sync>,
        setter: Arc<dyn ModelSetter>,
    ) -> Self {
        Self {
            field_def,
            extractor,
            setter,
        }
    }

    /// Apply this mapping to set a value on an Individual
    pub fn apply(&self, batch: &RecordBatch, row: usize, individual: &mut Individual) {
        if let Some(value) = (self.extractor)(batch, row) {
            (self.setter)(individual, value);
        }
    }
}

/// Factory for creating field extractors
pub struct Extractors;

impl Extractors {
    /// Create a string extractor for a field
    pub fn string(field_name: &str) -> Arc<dyn Fn(&RecordBatch, usize) -> Option<Box<dyn std::any::Any>> + Send + Sync> {
        let field_name = field_name.to_string();
        Arc::new(move |batch, row| {
            batch
                .column_by_name(&field_name)
                .and_then(|col| col.as_any().downcast_ref::<StringArray>())
                .and_then(|array| {
                    if row < array.len() && !array.is_null(row) {
                        Some(Box::new(array.value(row).to_string()) as Box<dyn std::any::Any>)
                    } else {
                        None
                    }
                })
        })
    }

    /// Create an integer extractor for a field
    pub fn integer(field_name: &str) -> Arc<dyn Fn(&RecordBatch, usize) -> Option<Box<dyn std::any::Any>> + Send + Sync> {
        let field_name = field_name.to_string();
        Arc::new(move |batch, row| {
            batch
                .column_by_name(&field_name)
                .and_then(|col| col.as_any().downcast_ref::<Int32Array>())
                .and_then(|array| {
                    if row < array.len() && !array.is_null(row) {
                        Some(Box::new(array.value(row)) as Box<dyn std::any::Any>)
                    } else {
                        None
                    }
                })
        })
    }

    /// Create a float extractor for a field
    pub fn decimal(field_name: &str) -> Arc<dyn Fn(&RecordBatch, usize) -> Option<Box<dyn std::any::Any>> + Send + Sync> {
        let field_name = field_name.to_string();
        Arc::new(move |batch, row| {
            batch
                .column_by_name(&field_name)
                .and_then(|col| col.as_any().downcast_ref::<Float64Array>())
                .and_then(|array| {
                    if row < array.len() && !array.is_null(row) {
                        Some(Box::new(array.value(row)) as Box<dyn std::any::Any>)
                    } else {
                        None
                    }
                })
        })
    }

    /// Create a boolean extractor for a field
    pub fn boolean(field_name: &str) -> Arc<dyn Fn(&RecordBatch, usize) -> Option<Box<dyn std::any::Any>> + Send + Sync> {
        let field_name = field_name.to_string();
        Arc::new(move |batch, row| {
            batch
                .column_by_name(&field_name)
                .and_then(|col| col.as_any().downcast_ref::<BooleanArray>())
                .and_then(|array| {
                    if row < array.len() && !array.is_null(row) {
                        Some(Box::new(array.value(row)) as Box<dyn std::any::Any>)
                    } else {
                        None
                    }
                })
        })
    }

    /// Create a date extractor for a field
    pub fn date(field_name: &str) -> Arc<dyn Fn(&RecordBatch, usize) -> Option<Box<dyn std::any::Any>> + Send + Sync> {
        let field_name = field_name.to_string();
        Arc::new(move |batch, row| {
            batch
                .column_by_name(&field_name)
                .and_then(|col| col.as_any().downcast_ref::<Date32Array>())
                .and_then(|array| {
                    if row < array.len() && !array.is_null(row) {
                        let days = array.value(row);
                        let date = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                            .unwrap()
                            .checked_add_signed(chrono::Duration::days(i64::from(days)));
                        
                        date.map(|d| Box::new(d) as Box<dyn std::any::Any>)
                    } else {
                        None
                    }
                })
        })
    }

    /// Create a generic extractor based on field type
    pub fn for_field(field_def: &FieldDefinition) -> Arc<dyn Fn(&RecordBatch, usize) -> Option<Box<dyn std::any::Any>> + Send + Sync> {
        match field_def.field_type {
            FieldType::PNR | FieldType::String | FieldType::Other => Self::string(&field_def.name),
            FieldType::Integer | FieldType::Category => Self::integer(&field_def.name),
            FieldType::Decimal => Self::decimal(&field_def.name),
            FieldType::Boolean => Self::boolean(&field_def.name),
            FieldType::Date => Self::date(&field_def.name),
        }
    }
}