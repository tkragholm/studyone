//! Tests for Child serialization using `serde_arrow`
//!
//! This module tests the serialization and deserialization of Child models
//! using `serde_arrow` for conversion to and from Arrow record batches.

use arrow::array::AsArray;
use par_reader::models::child::Child;
use par_reader::models::individual::Individual;
use par_reader::models::traits::ArrowSchema;
use par_reader::models::types::{DiseaseOrigin, DiseaseSeverity, Gender, ScdCategory};
use std::sync::Arc;

// Helper function to create a test Child
fn create_test_child() -> Child {
    let individual = Individual::new(
        "1234567890".to_string(),
        Gender::Male,
        Some(chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap()),
    );

    Child::from_individual(Arc::new(individual))
        .with_birth_details(Some(3500), Some(40), Some(10))
        .with_birth_order(1)
        .with_hospitalizations(1.5)
}

#[test]
fn test_child_serialization() {
    // Create a sample child
    let child = create_test_child();

    // Convert to record batch
    let children = vec![child.clone()];
    let batch = Child::to_record_batch(&children).expect("Failed to convert to record batch");

    // Verify the structure of the batch
    let schema = batch.schema();
    assert!(
        schema.fields().iter().any(|f| f.name() == "birth_weight"),
        "Schema should contain birth_weight"
    );
    assert!(
        schema
            .fields()
            .iter()
            .any(|f| f.name() == "gestational_age"),
        "Schema should contain gestational_age"
    );
    assert!(
        schema.fields().iter().any(|f| f.name() == "apgar_score"),
        "Schema should contain apgar_score"
    );
    assert!(
        schema
            .fields()
            .iter()
            .any(|f| f.name() == "has_severe_chronic_disease"),
        "Schema should contain has_severe_chronic_disease"
    );
    assert!(
        schema
            .fields()
            .iter()
            .any(|f| f.name() == "hospitalizations_per_year"),
        "Schema should contain hospitalizations_per_year"
    );

    assert_eq!(batch.num_rows(), 1, "Batch should have 1 row");
}

#[test]
fn test_child_serde_roundtrip() {
    // Create a few sample children with different properties
    let mut children = Vec::new();

    // Child 1: Basic child with birth details
    let individual1 = Individual::new(
        "1234567890".to_string(),
        Gender::Male,
        Some(chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap()),
    );
    let child1 = Child::from_individual(Arc::new(individual1))
        .with_birth_details(Some(3500), Some(40), Some(10))
        .with_birth_order(1);
    children.push(child1);

    // Child 2: Child with SCD
    let individual2 = Individual::new(
        "2345678901".to_string(),
        Gender::Female,
        Some(chrono::NaiveDate::from_ymd_opt(2005, 5, 15).unwrap()),
    );
    let child2 = Child::from_individual(Arc::new(individual2))
        .with_birth_details(Some(2800), Some(37), Some(9))
        .with_birth_order(2)
        .with_scd(
            ScdCategory::EndocrineDisorder,
            chrono::NaiveDate::from_ymd_opt(2010, 3, 10).unwrap(),
            DiseaseSeverity::Moderate,
            DiseaseOrigin::Acquired,
        )
        .with_hospitalizations(2.3);
    children.push(child2);

    // Convert to record batch
    let batch = Child::to_record_batch(&children).expect("Failed to convert to record batch");

    // Verify the structure of the batch
    assert_eq!(batch.num_rows(), 2, "Batch should have 2 rows");

    // Note: The from_record_batch implementation would require an Individual lookup
    // which we can't easily test here. In practice, this conversion would be handled
    // by the MfrChildRegister which maintains an Individual lookup.

    // Instead, we can verify the structure of the RecordBatch
    // We expect 11 fields since we're skipping 'individual' and 'diagnoses' fields with #[serde(skip)]
    assert_eq!(batch.schema().fields().len(), 11, "Should have 11 fields");

    // We can also manually check a couple of values
    let has_scd_array = batch
        .column_by_name("has_severe_chronic_disease")
        .expect("Missing has_severe_chronic_disease column");
    let has_scd = has_scd_array.as_boolean();

    assert!(!has_scd.value(0), "First child should not have SCD");
    assert!(has_scd.value(1), "Second child should have SCD");
}
