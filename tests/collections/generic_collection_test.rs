//! Tests for the generic collection implementation
//!
//! This module tests the functionality of the GenericCollection
//! implementation with various model types.

use chrono::NaiveDate;
use par_reader::collections::GenericCollection;
use par_reader::common::traits::{LookupCollection, ModelCollection, TemporalCollection};
use par_reader::models::{Diagnosis, Individual};

#[test]
fn test_generic_collection_basic_functions() {
    // Create a collection of individuals
    let mut collection = GenericCollection::<Individual>::new();

    // Create test individuals
    let individual1 = Individual {
        pnr: "1234567890".to_string(),
        gender: "M".into(),
        birth_date: Some(NaiveDate::from_ymd_opt(1990, 1, 1).unwrap()),
        death_date: None,
        municipality_code: Some("101".to_string()),
        is_rural: false,
        origin: "DK".into(),
        education_level: 3.into(),
        mother_pnr: None,
        father_pnr: None,
        family_id: None,
        emigration_date: None,
        immigration_date: None,
    };

    let individual2 = Individual {
        pnr: "0987654321".to_string(),
        gender: "F".into(),
        birth_date: Some(NaiveDate::from_ymd_opt(1985, 5, 5).unwrap()),
        death_date: None,
        municipality_code: Some("102".to_string()),
        is_rural: true,
        origin: "DK".into(),
        education_level: 4.into(),
        mother_pnr: None,
        father_pnr: None,
        family_id: None,
        emigration_date: None,
        immigration_date: None,
    };

    // Add individuals to collection
    collection.add(individual1.clone());
    collection.add(individual2.clone());

    // Test count
    assert_eq!(collection.count(), 2);

    // Test get by ID
    let retrieved = collection.get(&"1234567890".to_string()).unwrap();
    assert_eq!(retrieved.pnr, "1234567890");

    // Test all
    let all = collection.all();
    assert_eq!(all.len(), 2);

    // Test filter
    let males = collection.filter(|i| i.gender == "M".into());
    assert_eq!(males.len(), 1);
    assert_eq!(males[0].pnr, "1234567890");

    // Test contains
    assert!(collection.contains(&"1234567890".to_string()));
    assert!(!collection.contains(&"nonexistent".to_string()));
}

#[test]
fn test_lookup_collection() {
    // Create a collection of diagnoses
    let mut collection = GenericCollection::<Diagnosis>::new();

    // Create test diagnoses
    let diagnosis1 = Diagnosis {
        individual_pnr: "1234567890".to_string(),
        diagnosis_code: "A01".to_string(),
        diagnosis_type: 1.into(),
        diagnosis_date: Some(NaiveDate::from_ymd_opt(2020, 1, 15).unwrap()),
        is_scd: false,
        severity: 1,
    };

    let diagnosis2 = Diagnosis {
        individual_pnr: "1234567890".to_string(),
        diagnosis_code: "B02".to_string(),
        diagnosis_type: 2.into(),
        diagnosis_date: Some(NaiveDate::from_ymd_opt(2020, 2, 20).unwrap()),
        is_scd: true,
        severity: 2,
    };

    let diagnosis3 = Diagnosis {
        individual_pnr: "0987654321".to_string(),
        diagnosis_code: "C03".to_string(),
        diagnosis_type: 1.into(),
        diagnosis_date: Some(NaiveDate::from_ymd_opt(2020, 3, 25).unwrap()),
        is_scd: false,
        severity: 1,
    };

    // Add diagnoses to collection
    collection.add(diagnosis1);
    collection.add(diagnosis2);
    collection.add(diagnosis3);

    // Test create_lookup with a custom key function
    let lookup = collection.create_lookup(|d| d.individual_pnr.clone());

    // Should have 2 entries (last one for each PNR wins)
    assert_eq!(lookup.len(), 2);
    assert!(lookup.contains_key("1234567890"));
    assert!(lookup.contains_key("0987654321"));

    // Test create_multi_lookup
    let multi_lookup = collection.create_multi_lookup(|d| d.individual_pnr.clone());

    // Should have 2 entries
    assert_eq!(multi_lookup.len(), 2);
    assert_eq!(multi_lookup.get("1234567890").unwrap().len(), 2);
    assert_eq!(multi_lookup.get("0987654321").unwrap().len(), 1);
}

#[test]
fn test_temporal_collection() {
    // Create test individuals with different validity periods
    let individual1 = Individual {
        pnr: "1234567890".to_string(),
        gender: "M".into(),
        birth_date: Some(NaiveDate::from_ymd_opt(1990, 1, 1).unwrap()),
        death_date: Some(NaiveDate::from_ymd_opt(2020, 1, 1).unwrap()),
        municipality_code: Some("101".to_string()),
        is_rural: false,
        origin: "DK".into(),
        education_level: 3.into(),
        mother_pnr: None,
        father_pnr: None,
        family_id: None,
        emigration_date: None,
        immigration_date: None,
    };

    let individual2 = Individual {
        pnr: "0987654321".to_string(),
        gender: "F".into(),
        birth_date: Some(NaiveDate::from_ymd_opt(1985, 5, 5).unwrap()),
        death_date: None,
        municipality_code: Some("102".to_string()),
        is_rural: true,
        origin: "DK".into(),
        education_level: 4.into(),
        mother_pnr: None,
        father_pnr: None,
        family_id: None,
        emigration_date: None,
        immigration_date: None,
    };

    // Create a collection and add individuals
    let mut collection = GenericCollection::<Individual>::new();
    collection.add(individual1);
    collection.add(individual2);

    // Test the temporal functions

    // Both should be valid in 2010
    let date_2010 = NaiveDate::from_ymd_opt(2010, 1, 1).unwrap();
    let valid_2010 = collection.valid_at(&date_2010);
    assert_eq!(valid_2010.len(), 2);

    // Only one should be valid in 2022 (after the first one's death)
    let date_2022 = NaiveDate::from_ymd_opt(2022, 1, 1).unwrap();
    let valid_2022 = collection.valid_at(&date_2022);
    assert_eq!(valid_2022.len(), 1);
    assert_eq!(valid_2022[0].pnr, "0987654321");

    // Test valid_during
    let start_date = NaiveDate::from_ymd_opt(2015, 1, 1).unwrap();
    let end_date = NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();
    let valid_range = collection.valid_during(&start_date, &end_date);
    assert_eq!(valid_range.len(), 2);
}
