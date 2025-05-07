//! Tests for the direct registry-model integration
//!
//! This module contains tests that verify the correct functioning of the
//! direct integration between registry types and domain models.

use crate::registry::{BefRegister, ModelConversion, ModelConversionExt, RegisterLoader};
use crate::models::{Individual, Family};
use arrow::array::{Date32Array, Int8Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

/// Create a test BEF record batch for testing
fn create_test_bef_batch() -> RecordBatch {
    // Define schema matching BEF registry
    let schema = Schema::new(vec![
        Field::new("PNR", DataType::Utf8, false),
        Field::new("FOED_DAG", DataType::Date32, true),
        Field::new("FAR_ID", DataType::Utf8, true),
        Field::new("MOR_ID", DataType::Utf8, true),
        Field::new("FAMILIE_ID", DataType::Utf8, true),
        Field::new("KOEN", DataType::Utf8, true),
        Field::new("KOM", DataType::Int8, true),
        Field::new("OPR_LAND", DataType::Utf8, true),
    ]);

    // Create arrays for each column
    let pnr_array = StringArray::from(vec!["1234567890", "2345678901", "3456789012"]);
    
    // Date32 days since 1970-01-01 (representing birth dates)
    let birth_date_array = Date32Array::from(vec![
        Some(365 * 30), // ~30 years ago
        Some(365 * 25), // ~25 years ago
        Some(365 * 5),  // ~5 years ago
    ]);
    
    let far_id_array = StringArray::from(vec![Some("2345678901"), None, Some("2345678901")]);
    let mor_id_array = StringArray::from(vec![None, Some("1234567890"), Some("1234567890")]);
    
    let familie_id_array = StringArray::from(vec![
        Some("FAM001"),
        Some("FAM001"),
        Some("FAM001"),
    ]);
    
    let gender_array = StringArray::from(vec![Some("F"), Some("M"), Some("M")]);
    
    let municipality_array = Int8Array::from(vec![Some(101), Some(101), Some(101)]);
    
    let origin_array = StringArray::from(vec![Some("5100"), Some("5100"), Some("5100")]);
    
    // Combine into record batch
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(pnr_array),
            Arc::new(birth_date_array),
            Arc::new(far_id_array),
            Arc::new(mor_id_array),
            Arc::new(familie_id_array),
            Arc::new(gender_array),
            Arc::new(municipality_array),
            Arc::new(origin_array),
        ],
    )
    .unwrap()
}

#[test]
fn test_direct_conversion_to_individuals() {
    // Create test data
    let batch = create_test_bef_batch();
    
    // Create registry
    let bef_registry = BefRegister::new();
    
    // Convert directly to individuals using ModelConversion trait
    let individuals = bef_registry.to_models::<Individual>(&batch).unwrap();
    
    // Verify results
    assert_eq!(individuals.len(), 3, "Should convert 3 individuals");
    
    // Verify first individual
    let ind1 = &individuals[0];
    assert_eq!(ind1.pnr, "1234567890");
    assert_eq!(ind1.gender, crate::models::individual::Gender::Female);
    assert!(ind1.birth_date.is_some());
    assert_eq!(ind1.father_pnr, Some("2345678901".to_string()));
    assert_eq!(ind1.mother_pnr, None);
    assert_eq!(ind1.family_id, Some("FAM001".to_string()));
    
    // Verify second individual
    let ind2 = &individuals[1];
    assert_eq!(ind2.pnr, "2345678901");
    assert_eq!(ind2.gender, crate::models::individual::Gender::Male);
    assert!(ind2.birth_date.is_some());
    assert_eq!(ind2.father_pnr, None);
    assert_eq!(ind2.mother_pnr, Some("1234567890".to_string()));
    assert_eq!(ind2.family_id, Some("FAM001".to_string()));
}

#[test]
fn test_direct_conversion_to_families() {
    // Create test data
    let batch = create_test_bef_batch();
    
    // Create registry
    let bef_registry = BefRegister::new();
    
    // Convert directly to families using ModelConversion trait
    let families = bef_registry.to_models::<Family>(&batch).unwrap();
    
    // Verify results
    assert_eq!(families.len(), 1, "Should create 1 family");
    
    // Verify family details
    let family = &families[0];
    assert_eq!(family.id, "FAM001");
    assert_eq!(family.family_type, crate::models::family::FamilyType::TwoParent);
}

#[test]
fn test_batch_conversion_consistency() {
    // Create test data
    let batch = create_test_bef_batch();
    
    // Create registry
    let bef_registry = BefRegister::new();
    
    // Get both individuals and families in a consistent way
    let individuals = bef_registry.to_models::<Individual>(&batch).unwrap();
    let families = bef_registry.to_models::<Family>(&batch).unwrap();
    
    // Verify family individuals match
    assert_eq!(families.len(), 1, "Should be one family");
    assert_eq!(individuals.len(), 3, "Should be three individuals");
    
    // Check that all individuals have the same family_id
    for individual in &individuals {
        assert_eq!(individual.family_id, Some("FAM001".to_string()));
    }
}