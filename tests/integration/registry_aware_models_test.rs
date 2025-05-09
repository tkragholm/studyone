//! Tests for registry-aware model implementations
//!
//! This module contains tests to verify the registry-aware model implementations
//! which centralize registry-specific behavior in registry files instead of models.

use arrow::array::{BooleanArray, Date32Array, Int8Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use par_reader::common::traits::{BefRegistry, MfrRegistry, RegistryAware};
use par_reader::models::{Child, Individual};
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

    let familie_id_array = StringArray::from(vec![Some("FAM001"), Some("FAM001"), Some("FAM001")]);

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
fn test_registry_aware_model_implementation() {
    // Create test data
    let batch = create_test_bef_batch();

    // Test registry-aware from_registry_record implementation
    let individual = Individual::from_registry_record(&batch, 0).unwrap();

    // Verify results
    assert!(individual.is_some(), "Should convert individual");
    let individual = individual.unwrap();
    assert_eq!(individual.pnr, "1234567890", "PNR should match");
    assert_eq!(
        individual.father_pnr,
        Some("2345678901".to_string()),
        "Father PNR should match"
    );
    assert_eq!(individual.mother_pnr, None, "Mother PNR should be None");
    assert_eq!(
        individual.family_id,
        Some("FAM001".to_string()),
        "Family ID should match"
    );

    // Test registry-aware from_registry_batch implementation
    let individuals = Individual::from_registry_batch(&batch).unwrap();
    assert_eq!(individuals.len(), 3, "Should convert 3 individuals");

    // Test BefRegistry implementation
    let individuals_from_bef = Individual::from_bef_batch(&batch).unwrap();
    assert_eq!(
        individuals_from_bef.len(),
        3,
        "Should convert 3 individuals from BEF"
    );

    // Verify first individual from BEF
    let ind1 = &individuals_from_bef[0];
    assert_eq!(ind1.pnr, "1234567890");
    assert_eq!(ind1.gender, par_reader::models::types::Gender::Female);

    // Test registry name
    assert_eq!(
        Individual::registry_name(),
        "BEF",
        "Registry name should be BEF"
    );
}

/// Create a test MFR record batch for testing
fn create_test_mfr_batch() -> RecordBatch {
    // Define schema matching MFR registry
    let schema = Schema::new(vec![
        Field::new("PNR", DataType::Utf8, false),
        Field::new("BIRTH_WEIGHT", DataType::Int32, true),
        Field::new("GESTATIONAL_AGE", DataType::Int32, true),
        Field::new("HAS_SCD", DataType::Boolean, false),
    ]);

    // Create arrays for each column
    let pnr_array = StringArray::from(vec!["1234567890", "2345678901", "3456789012"]);

    // Birth weight in grams
    let birth_weight_array =
        arrow::array::Int32Array::from(vec![Some(3500), Some(2800), Some(4100)]);

    // Gestational age in weeks
    let gestational_age_array = arrow::array::Int32Array::from(vec![Some(40), Some(37), Some(38)]);

    // Has severe chronic disease flag
    let has_scd_array = BooleanArray::from(vec![false, true, false]);

    // Combine into record batch
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(pnr_array),
            Arc::new(birth_weight_array),
            Arc::new(gestational_age_array),
            Arc::new(has_scd_array),
        ],
    )
    .unwrap()
}

#[test]
fn test_child_registry_aware_implementation() {
    // Create test data
    let batch = create_test_mfr_batch();

    // Test registry-aware from_registry_record implementation
    let child = Child::from_registry_record(&batch, 0).unwrap();

    // Verify results
    assert!(child.is_some(), "Should convert child");
    let child = child.unwrap();
    assert_eq!(child.individual().pnr, "1234567890", "PNR should match");

    // Test registry-aware from_registry_batch implementation
    let children = Child::from_registry_batch(&batch).unwrap();
    assert_eq!(children.len(), 3, "Should convert 3 children");

    // Test MfrRegistry implementation
    let children_from_mfr = Child::from_mfr_batch(&batch).unwrap();
    assert_eq!(
        children_from_mfr.len(),
        3,
        "Should convert 3 children from MFR"
    );

    // Verify first child from MFR
    let child1 = &children_from_mfr[0];
    assert_eq!(child1.individual().pnr, "1234567890");

    // Test registry name
    assert_eq!(Child::registry_name(), "MFR", "Registry name should be MFR");
}
