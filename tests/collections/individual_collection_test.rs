//! Tests for the specialized IndividualCollection implementation
//!
//! This module tests the functionality of the IndividualCollection,
//! focusing on the specialized methods it provides.

use par_reader::collections::IndividualCollection;
use par_reader::common::traits::ModelCollection;
use par_reader::models::Individual;
use chrono::NaiveDate;

#[test]
fn test_individual_collection() {
    // Create test individuals
    let individual1 = Individual {
        pnr: "1234567890".to_string(),
        gender: "M".to_string(),
        birth_date: NaiveDate::from_ymd_opt(1990, 1, 1).unwrap(),
        death_date: Some(NaiveDate::from_ymd_opt(2020, 1, 1).unwrap()),
        municipality_code: Some("101".to_string()),
        is_rural: false,
        origin: "DK".to_string(),
        education_level: Some(3),
        income: None,
        diagnosis_history: Vec::new(),
    };
    
    let individual2 = Individual {
        pnr: "0987654321".to_string(),
        gender: "F".to_string(),
        birth_date: NaiveDate::from_ymd_opt(1985, 5, 5).unwrap(),
        death_date: None,
        municipality_code: Some("102".to_string()),
        is_rural: true,
        origin: "DK".to_string(),
        education_level: Some(4),
        income: None,
        diagnosis_history: Vec::new(),
    };
    
    let individual3 = Individual {
        pnr: "5432167890".to_string(),
        gender: "F".to_string(),
        birth_date: NaiveDate::from_ymd_opt(2015, 3, 3).unwrap(),
        death_date: None,
        municipality_code: Some("101".to_string()),
        is_rural: false,
        origin: "DK".to_string(),
        education_level: None, // Child, no education level
        income: None,
        diagnosis_history: Vec::new(),
    };
    
    // Create collection and add individuals
    let mut collection = IndividualCollection::new();
    collection.add(individual1);
    collection.add(individual2);
    collection.add(individual3);
    
    // Test specialized collection methods
    
    // Test alive_at method
    let date_2015 = NaiveDate::from_ymd_opt(2015, 6, 1).unwrap();
    let alive_2015 = collection.alive_at(&date_2015);
    assert_eq!(alive_2015.len(), 3);
    
    let date_2022 = NaiveDate::from_ymd_opt(2022, 1, 1).unwrap();
    let alive_2022 = collection.alive_at(&date_2022);
    assert_eq!(alive_2022.len(), 2);
    
    // Test age_between method
    let adults = collection.age_between(&date_2022, 18, 65);
    assert_eq!(adults.len(), 1);
    assert_eq!(adults[0].pnr, "0987654321");
    
    let children = collection.age_between(&date_2022, 0, 17);
    assert_eq!(children.len(), 1);
    assert_eq!(children[0].pnr, "5432167890");
    
    // Test by_gender method
    let females = collection.by_gender("F");
    assert_eq!(females.len(), 2);
    
    let males = collection.by_gender("M");
    assert_eq!(males.len(), 1);
    
    // Test by_municipality method
    let muni_101 = collection.by_municipality("101");
    assert_eq!(muni_101.len(), 2);
}

#[test]
fn test_individual_collection_from_individuals() {
    // Create test individuals
    let individuals = vec![
        Individual {
            pnr: "1234567890".to_string(),
            gender: "M".to_string(),
            birth_date: NaiveDate::from_ymd_opt(1990, 1, 1).unwrap(),
            death_date: None,
            municipality_code: Some("101".to_string()),
            is_rural: false,
            origin: "DK".to_string(),
            education_level: Some(3),
            income: None,
            diagnosis_history: Vec::new(),
        },
        Individual {
            pnr: "0987654321".to_string(),
            gender: "F".to_string(),
            birth_date: NaiveDate::from_ymd_opt(1985, 5, 5).unwrap(),
            death_date: None,
            municipality_code: Some("102".to_string()),
            is_rural: true,
            origin: "DK".to_string(),
            education_level: Some(4),
            income: None,
            diagnosis_history: Vec::new(),
        },
    ];
    
    // Create collection from vector
    let collection = IndividualCollection::from_individuals(individuals);
    
    // Test that all individuals were added
    assert_eq!(collection.count(), 2);
    assert!(collection.contains(&"1234567890".to_string()));
    assert!(collection.contains(&"0987654321".to_string()));
}