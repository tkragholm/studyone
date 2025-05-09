//! Tests for the enhanced registry integration with serde_arrow
//!
//! This module tests the functionality of Individual and Child models
//! with registry data using the new serde_arrow integration.

use arrow::array::{ArrayRef, StringArray, Int32Array, Float64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use par_reader::models::core::Individual;
use par_reader::models::core::types::Gender;
use par_reader::models::derived::Child;
use std::sync::Arc;

// Helper function to create a test BEF-like registry batch
fn create_bef_test_batch() -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("PNR", DataType::Utf8, false),
        Field::new("KOEN", DataType::Utf8, false),
        Field::new("FOED_DAG", DataType::Int32, true),
        Field::new("FAMILIE_ID", DataType::Utf8, true),
        Field::new("MOR_ID", DataType::Utf8, true),
        Field::new("FAR_ID", DataType::Utf8, true),
    ]);
    
    // Create arrays for the batch (2 individuals)
    let pnr_array: ArrayRef = Arc::new(StringArray::from(vec!["1234567890", "2345678901"]));
    let koen_array: ArrayRef = Arc::new(StringArray::from(vec!["M", "F"]));
    
    // Convert dates to days since epoch (1970-01-01)
    let date1 = NaiveDate::from_ymd_opt(2010, 5, 12).unwrap();
    let date2 = NaiveDate::from_ymd_opt(2012, 8, 30).unwrap();
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let days1 = (date1 - epoch).num_days() as i32;
    let days2 = (date2 - epoch).num_days() as i32;
    
    let birth_array: ArrayRef = Arc::new(Int32Array::from(vec![Some(days1), Some(days2)]));
    let family_array: ArrayRef = Arc::new(StringArray::from(vec![Some("FAM001"), Some("FAM002")]));
    let mother_array: ArrayRef = Arc::new(StringArray::from(vec![Some("9876543210"), Some("8765432109")]));
    let father_array: ArrayRef = Arc::new(StringArray::from(vec![Some("8765432101"), Some("7654321098")]));
    
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            pnr_array,
            koen_array, 
            birth_array, 
            family_array,
            mother_array,
            father_array,
        ],
    ).unwrap()
}

// Helper function to create a test MFR-like registry batch
fn create_mfr_test_batch() -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("PNR", DataType::Utf8, false),
        Field::new("BARSELNR", DataType::Int32, false), // Marker field for MFR registry
        Field::new("VAEGT", DataType::Int32, true),
        Field::new("LAENGDE", DataType::Int32, true),
        Field::new("APGAR5", DataType::Int32, true),
        Field::new("SVLENGTH", DataType::Int32, true),
        Field::new("MOR_CPR", DataType::Utf8, true),
        Field::new("FAR_CPR", DataType::Utf8, true),
    ]);
    
    // Create arrays for the batch (2 children)
    let pnr_array: ArrayRef = Arc::new(StringArray::from(vec!["1234567890", "2345678901"]));
    let barselnr_array: ArrayRef = Arc::new(Int32Array::from(vec![12345, 23456]));
    let weight_array: ArrayRef = Arc::new(Int32Array::from(vec![Some(3500), Some(3200)]));
    let length_array: ArrayRef = Arc::new(Int32Array::from(vec![Some(52), Some(50)]));
    let apgar_array: ArrayRef = Arc::new(Int32Array::from(vec![Some(10), Some(9)]));
    let ga_array: ArrayRef = Arc::new(Int32Array::from(vec![Some(40), Some(38)]));
    let mother_array: ArrayRef = Arc::new(StringArray::from(vec![Some("9876543210"), Some("8765432109")]));
    let father_array: ArrayRef = Arc::new(StringArray::from(vec![Some("8765432101"), Some("7654321098")]));
    
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            pnr_array,
            barselnr_array,
            weight_array,
            length_array,
            apgar_array,
            ga_array,
            mother_array,
            father_array,
        ],
    ).unwrap()
}

// Helper function to create a test IND-like registry batch
fn create_ind_test_batch() -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("PNR", DataType::Utf8, false),
        Field::new("PERINDKIALT", DataType::Float64, true), // Marker field for IND registry
        Field::new("DISPON_NY", DataType::Float64, true),
        Field::new("LOENMV", DataType::Float64, true),
        Field::new("AAR", DataType::Int32, true),
    ]);
    
    // Create arrays for the batch (2 individuals)
    let pnr_array: ArrayRef = Arc::new(StringArray::from(vec!["1234567890", "2345678901"]));
    let income_array: ArrayRef = Arc::new(Float64Array::from(vec![Some(500000.0), Some(450000.0)]));
    let disposable_array: ArrayRef = Arc::new(Float64Array::from(vec![Some(300000.0), Some(270000.0)]));
    let salary_array: ArrayRef = Arc::new(Float64Array::from(vec![Some(450000.0), Some(400000.0)]));
    let year_array: ArrayRef = Arc::new(Int32Array::from(vec![Some(2022), Some(2022)]));
    
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            pnr_array,
            income_array,
            disposable_array,
            salary_array,
            year_array,
        ],
    ).unwrap()
}

#[test]
fn test_individual_from_bef_registry() {
    let batch = create_bef_test_batch();
    
    // Test row-by-row conversion
    let individual1 = Individual::from_registry_record(&batch, 0).unwrap().unwrap();
    let individual2 = Individual::from_registry_record(&batch, 1).unwrap().unwrap();
    
    // Verify core attributes
    assert_eq!(individual1.pnr, "1234567890");
    assert_eq!(individual2.pnr, "2345678901");
    assert_eq!(individual1.gender, Gender::Male);
    assert_eq!(individual2.gender, Gender::Female);
    
    // Verify family relationships
    assert_eq!(individual1.family_id, Some("FAM001".to_string()));
    assert_eq!(individual2.family_id, Some("FAM002".to_string()));
    assert_eq!(individual1.mother_pnr, Some("9876543210".to_string()));
    assert_eq!(individual2.mother_pnr, Some("8765432109".to_string()));
    
    // Test batch conversion
    let individuals = Individual::from_registry_batch(&batch).unwrap();
    assert_eq!(individuals.len(), 2);
    assert_eq!(individuals[0].pnr, "1234567890");
    assert_eq!(individuals[1].pnr, "2345678901");
}

#[test]
fn test_individual_from_mfr_registry() {
    let batch = create_mfr_test_batch();
    
    // Test row-by-row conversion
    let individual = Individual::from_registry_record(&batch, 0).unwrap().unwrap();
    
    // Verify core attributes
    assert_eq!(individual.pnr, "1234567890");
    
    // Test batch conversion
    let individuals = Individual::from_registry_batch(&batch).unwrap();
    assert_eq!(individuals.len(), 2);
    assert_eq!(individuals[0].pnr, "1234567890");
    assert_eq!(individuals[1].pnr, "2345678901");
}

#[test]
fn test_individual_from_ind_registry() {
    let batch = create_ind_test_batch();
    
    // Test row-by-row conversion
    let individual = Individual::from_registry_record(&batch, 0).unwrap().unwrap();
    
    // Verify core attributes
    assert_eq!(individual.pnr, "1234567890");
    
    // Test batch conversion
    let individuals = Individual::from_registry_batch(&batch).unwrap();
    assert_eq!(individuals.len(), 2);
    assert_eq!(individuals[0].pnr, "1234567890");
    assert_eq!(individuals[1].pnr, "2345678901");
}

#[test]
fn test_individual_enhance_from_registry() {
    // Create an individual with minimal data
    let mut individual = Individual::new(
        "1234567890".to_string(), 
        Gender::Unknown,
        None,
    );
    
    // Enhance with BEF data
    let bef_batch = create_bef_test_batch();
    let enhanced = individual.enhance_from_registry(&bef_batch, 0).unwrap();
    
    assert!(enhanced);
    assert_eq!(individual.gender, Gender::Male);
    assert_eq!(individual.family_id, Some("FAM001".to_string()));
    assert_eq!(individual.mother_pnr, Some("9876543210".to_string()));
    assert_eq!(individual.father_pnr, Some("8765432101".to_string()));
    
    // Further enhance with IND data
    let ind_batch = create_ind_test_batch();
    let enhanced = individual.enhance_from_registry(&ind_batch, 0).unwrap();
    
    assert!(enhanced);
    assert_eq!(individual.annual_income, Some(500000.0));
    assert_eq!(individual.disposable_income, Some(300000.0));
    assert_eq!(individual.employment_income, Some(450000.0));
    assert_eq!(individual.income_year, Some(2022));
}

#[test]
fn test_child_from_registry() {
    // Create child from MFR registry
    let batch = create_mfr_test_batch();
    let child = Child::from_registry_record(&batch, 0).unwrap().unwrap();
    
    // Verify underlying individual data
    assert_eq!(child.individual().pnr, "1234567890");
    
    // Verify child-specific data
    assert_eq!(child.birth_weight, Some(3500));
    assert_eq!(child.gestational_age, Some(40));
    assert_eq!(child.apgar_score, Some(10));
    
    // Test batch conversion
    let children = Child::from_registry_batch(&batch).unwrap();
    assert_eq!(children.len(), 2);
    assert_eq!(children[0].individual().pnr, "1234567890");
    assert_eq!(children[1].individual().pnr, "2345678901");
    assert_eq!(children[0].birth_weight, Some(3500));
    assert_eq!(children[1].birth_weight, Some(3200));
}

#[test]
fn test_serde_arrow_integration() {
    // Create a registry batch
    let batch = create_bef_test_batch();
    
    // Convert to Individuals using serde_arrow
    let individuals = Individual::from_registry_batch_with_serde_arrow(&batch).unwrap();
    assert_eq!(individuals.len(), 2);
    assert_eq!(individuals[0].pnr, "1234567890");
    assert_eq!(individuals[1].pnr, "2345678901");
    
    // Convert Individuals back to a record batch
    let result_batch = Individual::to_record_batch(&individuals).unwrap();
    assert_eq!(result_batch.num_rows(), 2);
    
    // Extract a column and verify values
    let pnr_col = result_batch.column_by_name("pnr").unwrap();
    let pnr_array = pnr_col.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(pnr_array.value(0), "1234567890");
    assert_eq!(pnr_array.value(1), "2345678901");
}