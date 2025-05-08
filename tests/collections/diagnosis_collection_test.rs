//! Tests for the specialized DiagnosisCollection implementation
//!
//! This module tests the functionality of the DiagnosisCollection,
//! focusing on the specialized methods it provides.

use par_reader::collections::DiagnosisCollection;
use par_reader::common::traits::ModelCollection;
use par_reader::models::diagnosis::{Diagnosis, ScdResult};
use chrono::NaiveDate;

#[test]
fn test_diagnosis_collection() {
    // Create test diagnoses
    let diagnosis1 = Diagnosis {
        individual_pnr: "1234567890".to_string(),
        diagnosis_code: "E10".to_string(), // Type 1 diabetes - SCD
        diagnosis_type: 1,
        diagnosis_date: Some(NaiveDate::from_ymd_opt(2020, 1, 15).unwrap()),
        is_scd: true,
        severity: 2,
    };
    
    let diagnosis2 = Diagnosis {
        individual_pnr: "1234567890".to_string(),
        diagnosis_code: "J45".to_string(), // Asthma - not SCD in this test
        diagnosis_type: 2,
        diagnosis_date: Some(NaiveDate::from_ymd_opt(2020, 2, 20).unwrap()),
        is_scd: false,
        severity: 1,
    };
    
    let diagnosis3 = Diagnosis {
        individual_pnr: "0987654321".to_string(),
        diagnosis_code: "C50".to_string(), // Breast cancer - SCD
        diagnosis_type: 1,
        diagnosis_date: Some(NaiveDate::from_ymd_opt(2020, 3, 25).unwrap()),
        is_scd: true,
        severity: 3,
    };
    
    // Create collection and add diagnoses
    let mut collection = DiagnosisCollection::new();
    collection.add(diagnosis1.clone());
    collection.add(diagnosis2.clone());
    collection.add(diagnosis3.clone());
    
    // Test get_diagnoses method
    let person1_diagnoses = collection.get_diagnoses("1234567890");
    assert_eq!(person1_diagnoses.len(), 2);
    
    let person2_diagnoses = collection.get_diagnoses("0987654321");
    assert_eq!(person2_diagnoses.len(), 1);
    
    // Test SCD results
    
    // Create SCD results
    let scd_result1 = ScdResult {
        pnr: "1234567890".to_string(),
        has_scd: true,
        first_scd_date: Some(NaiveDate::from_ymd_opt(2020, 1, 15).unwrap()),
        scd_diagnoses: vec![diagnosis1.into()],
        scd_categories: vec![3], // Type 1 diabetes category
        max_severity: 2,
        has_congenital: false,
        hospitalization_count: 1,
    };
    
    let scd_result2 = ScdResult {
        pnr: "0987654321".to_string(),
        has_scd: true,
        first_scd_date: Some(NaiveDate::from_ymd_opt(2020, 3, 25).unwrap()),
        scd_diagnoses: vec![diagnosis3.into()],
        scd_categories: vec![1], // Cancer category
        max_severity: 3,
        has_congenital: false,
        hospitalization_count: 2,
    };
    
    // Add SCD results to collection
    collection.add_scd_result(scd_result1);
    collection.add_scd_result(scd_result2);
    
    // Test SCD retrieval methods
    
    // Test get_scd_result
    let person1_scd = collection.get_scd_result("1234567890").unwrap();
    assert!(person1_scd.has_scd);
    assert_eq!(person1_scd.max_severity, 2);
    
    // Test individuals_with_scd
    let with_scd = collection.individuals_with_scd();
    assert_eq!(with_scd.len(), 2);
    assert!(with_scd.contains(&"1234567890".to_string()));
    assert!(with_scd.contains(&"0987654321".to_string()));
    
    // Test individuals_with_category
    let with_diabetes = collection.individuals_with_category(3);
    assert_eq!(with_diabetes.len(), 1);
    assert_eq!(with_diabetes[0], "1234567890");
    
    let with_cancer = collection.individuals_with_category(1);
    assert_eq!(with_cancer.len(), 1);
    assert_eq!(with_cancer[0], "0987654321");
    
    // Test scd_count
    assert_eq!(collection.scd_count(), 2);
    
    // Test count_by_severity
    let severity_counts = collection.count_by_severity();
    assert_eq!(severity_counts.get(&2), Some(&1)); // One person with severity 2
    assert_eq!(severity_counts.get(&3), Some(&1)); // One person with severity 3
}

#[test]
fn test_diagnosis_collection_from_diagnoses() {
    // Create test diagnoses
    let diagnoses = vec![
        Diagnosis {
            individual_pnr: "1234567890".to_string(),
            diagnosis_code: "E10".to_string(),
            diagnosis_type: 1,
            diagnosis_date: Some(NaiveDate::from_ymd_opt(2020, 1, 15).unwrap()),
            is_scd: true,
            severity: 2,
        },
        Diagnosis {
            individual_pnr: "0987654321".to_string(),
            diagnosis_code: "C50".to_string(),
            diagnosis_type: 1,
            diagnosis_date: Some(NaiveDate::from_ymd_opt(2020, 3, 25).unwrap()),
            is_scd: true,
            severity: 3,
        },
    ];
    
    // Create collection from vector
    let collection = DiagnosisCollection::from_diagnoses(diagnoses);
    
    // Test that all diagnoses were added
    assert_eq!(collection.count(), 2);
    
    // Test that by-PNR lookup works
    assert_eq!(collection.get_diagnoses("1234567890").len(), 1);
    assert_eq!(collection.get_diagnoses("0987654321").len(), 1);
}