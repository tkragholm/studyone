//! Secondary diagnosis handling for LPR data
//!
//! This module provides structures and functions for enhanced handling of secondary diagnoses
//! from the Danish National Patient Registry (LPR).

use arrow::array::{ArrayRef, Float32Array, ListArray, StringArray, StructArray};
use arrow::buffer::{BooleanBuffer, NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field};
use std::sync::Arc;

/// Represents a secondary diagnosis with its code, type, and weight
#[derive(Debug, Clone)]
pub struct SecondaryDiagnosis {
    /// The diagnosis code (ICD-10)
    pub code: String,
    /// The diagnosis type (e.g., "B" for bi-diagnosis)
    pub diagnosis_type: String,
    /// The relative weight/importance of this diagnosis (0.0-1.0)
    pub weight: f32,
}

impl SecondaryDiagnosis {
    /// Create a new secondary diagnosis
    #[must_use] pub fn new(code: String, diagnosis_type: String, weight: Option<f32>) -> Self {
        let weight = weight.unwrap_or({
            // Calculate weight based on diagnosis type
            match diagnosis_type.as_str() {
                "B" => 0.8, // Higher weight for bi-diagnoses
                "C" => 0.7, // Complications
                "G" => 0.6, // Grundmorbus
                "H" => 0.5, // Referring diagnosis
                "M" => 0.4, // Temporary diagnosis
                _ => 0.3,   // Default weight for unknown types
            }
        });

        Self {
            code,
            diagnosis_type,
            weight,
        }
    }
}

/// Create Arrow array for a list of secondary diagnoses
#[must_use] pub fn create_secondary_diagnoses_array(
    diagnoses_list: &[Option<Vec<SecondaryDiagnosis>>],
) -> ArrayRef {
    // Create field for the inner struct
    let struct_fields = vec![
        Field::new("code", DataType::Utf8, false),
        Field::new("diagnosis_type", DataType::Utf8, true),
        Field::new("weight", DataType::Float32, true),
    ];

    let struct_type = DataType::Struct(struct_fields.into());

    // Create builder for the list array
    let mut code_values = Vec::new();
    let mut code_offsets = Vec::new();
    let mut diagnosis_type_values = Vec::new();
    let mut diagnosis_type_offsets = Vec::new();
    let mut weight_values = Vec::new();
    let mut weight_offsets = Vec::new();

    // Current length tracking for the offset vectors
    let mut current_code_length = 0;
    let mut current_diagnosis_type_length = 0;
    let mut current_weight_length = 0;

    // Need to track null vs empty list
    let mut list_validity = Vec::with_capacity(diagnoses_list.len());
    let mut list_offsets = Vec::with_capacity(diagnoses_list.len() + 1);
    list_offsets.push(0);

    for diagnoses_opt in diagnoses_list {
        if let Some(diagnoses) = diagnoses_opt {
            list_validity.push(true);

            // Add values for each diagnosis in this list
            for diagnosis in diagnoses {
                // Code is required
                code_values.push(Some(diagnosis.code.clone()));
                current_code_length += 1;

                // Type is optional
                diagnosis_type_values.push(Some(diagnosis.diagnosis_type.clone()));
                current_diagnosis_type_length += 1;

                // Weight is optional
                weight_values.push(Some(diagnosis.weight));
                current_weight_length += 1;
            }

            code_offsets.push(current_code_length);
            diagnosis_type_offsets.push(current_diagnosis_type_length);
            weight_offsets.push(current_weight_length);

            list_offsets.push(list_offsets.last().unwrap() + diagnoses.len());
        } else {
            // This list is null
            list_validity.push(false);
            list_offsets.push(*list_offsets.last().unwrap());
        }
    }

    // Create the individual arrays for struct fields
    let code_array = Arc::new(StringArray::from(code_values));
    let diagnosis_type_array = Arc::new(StringArray::from(diagnosis_type_values));
    let weight_array = Arc::new(Float32Array::from(weight_values));

    // Create struct array with Arc<Field> instead of Field
    let struct_array = StructArray::from(vec![
        (
            Arc::new(Field::new("code", DataType::Utf8, false)),
            code_array as ArrayRef,
        ),
        (
            Arc::new(Field::new("diagnosis_type", DataType::Utf8, true)),
            diagnosis_type_array as ArrayRef,
        ),
        (
            Arc::new(Field::new("weight", DataType::Float32, true)),
            weight_array as ArrayRef,
        ),
    ]);

    // Use ListArray::try_new instead with proper offset builders
    let values_array = Arc::new(struct_array);

    // Convert to i32 offsets
    let mut offsets_i32 = Vec::with_capacity(list_offsets.len());
    for &offset in &list_offsets {
        offsets_i32.push(offset as i32);
    }

    // Create list array
    let list_array = ListArray::try_new(
        Arc::new(Field::new("item", struct_type, false)),
        OffsetBuffer::new(offsets_i32.into()),
        values_array,
        Some(NullBuffer::new(BooleanBuffer::from(list_validity))),
    )
    .unwrap();

    Arc::new(list_array) as ArrayRef
}

/// Process secondary diagnoses with weights based on diagnosis type
///
/// This function converts raw diagnosis tuples into structured secondary diagnoses
/// with appropriate weights and types.
#[must_use] pub fn process_secondary_diagnoses(diagnoses: &[(String, String)]) -> Vec<SecondaryDiagnosis> {
    diagnoses
        .iter()
        .filter(|(_, diag_type)| diag_type != "A") // Filter out primary diagnoses
        .map(|(diag, diag_type)| {
            SecondaryDiagnosis::new(diag.clone(), diag_type.clone(), None)
        })
        .collect()
}

/// Create the Arrow schema field for secondary diagnoses
///
/// This function returns the field definition for a list of secondary diagnoses
/// that can be included in a schema.
#[must_use] pub fn create_secondary_diagnoses_field() -> Field {
    // Define secondary diagnosis struct fields
    let secondary_diag_fields = vec![
        Field::new("code", DataType::Utf8, false),
        Field::new("diagnosis_type", DataType::Utf8, true),
        Field::new("weight", DataType::Float32, true),
    ];
    
    // Create struct field
    let secondary_diag_struct = Field::new(
        "item", 
        DataType::Struct(secondary_diag_fields.into()), 
        false
    );
    
    // Create list field with the struct as item type
    Field::new(
        "secondary_diagnoses", 
        DataType::List(Arc::new(secondary_diag_struct)), 
        true
    )
}