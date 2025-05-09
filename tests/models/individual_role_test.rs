//! Tests for the enhanced Individual model functionality
//!
//! This module tests the role-based classification and utility methods
//! added to the Individual model.

use chrono::NaiveDate;
use par_reader::models::core::types::Gender;
use par_reader::models::core::Individual;

/// Helper function to create a test dataset of individuals
fn create_test_dataset() -> Vec<Individual> {
    let mut individuals = Vec::new();
    
    // Child 1 (5 years old)
    let child1 = Individual::new(
        "1234567890".to_string(),
        Gender::Male,
        Some(NaiveDate::from_ymd_opt(2018, 5, 10).unwrap()),
    );
    individuals.push(child1);
    
    // Child 2 (10 years old)
    let child2 = Individual::new(
        "2345678901".to_string(),
        Gender::Female,
        Some(NaiveDate::from_ymd_opt(2013, 8, 15).unwrap()),
    );
    individuals.push(child2);
    
    // Parent 1 (Mother of Child 1)
    let mut mother = Individual::new(
        "3456789012".to_string(),
        Gender::Female,
        Some(NaiveDate::from_ymd_opt(1985, 3, 22).unwrap()),
    );
    mother.family_id = Some("FAM001".to_string());
    individuals.push(mother);
    
    // Parent 2 (Father of Child 1)
    let mut father = Individual::new(
        "4567890123".to_string(),
        Gender::Male,
        Some(NaiveDate::from_ymd_opt(1982, 7, 8).unwrap()),
    );
    father.family_id = Some("FAM001".to_string());
    individuals.push(father);
    
    // Set parent relationships
    let mut child1_with_parents = individuals[0].clone();
    child1_with_parents.mother_pnr = Some("3456789012".to_string());
    child1_with_parents.father_pnr = Some("4567890123".to_string());
    child1_with_parents.family_id = Some("FAM001".to_string());
    individuals[0] = child1_with_parents;
    
    // Young parent (both child and parent, 17 years old)
    let mut young_parent = Individual::new(
        "5678901234".to_string(),
        Gender::Female,
        Some(NaiveDate::from_ymd_opt(2006, 6, 12).unwrap()),
    );
    young_parent.family_id = Some("FAM002".to_string());
    individuals.push(young_parent);
    
    // Child of young parent
    let mut infant = Individual::new(
        "6789012345".to_string(),
        Gender::Male,
        Some(NaiveDate::from_ymd_opt(2022, 11, 5).unwrap()),
    );
    infant.mother_pnr = Some("5678901234".to_string());
    infant.family_id = Some("FAM002".to_string());
    individuals.push(infant);
    
    individuals
}

#[test]
fn test_individual_role_classification() {
    let individuals = create_test_dataset();
    let reference_date = NaiveDate::from_ymd_opt(2023, 5, 15).unwrap();
    
    // Test is_child method
    assert!(individuals[0].is_child(&reference_date), "5-year-old should be classified as a child");
    assert!(individuals[1].is_child(&reference_date), "10-year-old should be classified as a child");
    assert!(!individuals[2].is_child(&reference_date), "38-year-old should not be classified as a child");
    assert!(!individuals[3].is_child(&reference_date), "41-year-old should not be classified as a child");
    assert!(individuals[4].is_child(&reference_date), "17-year-old should be classified as a child");
    assert!(individuals[5].is_child(&reference_date), "infant should be classified as a child");
    
    // Test is_parent_in_dataset method
    assert!(!individuals[0].is_parent_in_dataset(&individuals), "Child 1 should not be a parent");
    assert!(!individuals[1].is_parent_in_dataset(&individuals), "Child 2 should not be a parent");
    assert!(individuals[2].is_parent_in_dataset(&individuals), "Mother should be a parent");
    assert!(individuals[3].is_parent_in_dataset(&individuals), "Father should be a parent");
    assert!(individuals[4].is_parent_in_dataset(&individuals), "Young parent should be a parent");
    assert!(!individuals[5].is_parent_in_dataset(&individuals), "Infant should not be a parent");

    // Test role_at method    
    use par_reader::models::core::Role;
    
    assert_eq!(individuals[0].role_at(&reference_date, &individuals), Role::Child, 
               "Child 1 should have Child role");
    assert_eq!(individuals[2].role_at(&reference_date, &individuals), Role::Parent, 
               "Mother should have Parent role");
    assert_eq!(individuals[4].role_at(&reference_date, &individuals), Role::ChildAndParent, 
               "Young parent should have ChildAndParent role");
}

#[test]
fn test_derived_model_creation() {
    let individuals = create_test_dataset();
    
    // Test to_child method
    let child = individuals[0].to_child();
    assert_eq!(child.individual().pnr, "1234567890", "Child's PNR should match");
    
    // Test to_parent method
    let parent = individuals[2].to_parent();
    assert_eq!(parent.individual().pnr, "3456789012", "Parent's PNR should match");
}

#[test]
fn test_batch_processing() {
    let individuals = create_test_dataset();
    let reference_date = NaiveDate::from_ymd_opt(2023, 5, 15).unwrap();
    
    // Test group_by_family method
    let family_groups = Individual::group_by_family(&individuals);
    assert_eq!(family_groups.len(), 2, "Should have 2 families");
    assert_eq!(family_groups["FAM001"].len(), 3, "Family 1 should have 3 members");
    assert_eq!(family_groups["FAM002"].len(), 2, "Family 2 should have 2 members");
    
    // Test create_children method
    let children = Individual::create_children(&individuals, &reference_date);
    assert_eq!(children.len(), 4, "Should create 4 child models");
    
    // Test create_parents method
    let parents = Individual::create_parents(&individuals);
    assert_eq!(parents.len(), 3, "Should create 3 parent models");
    
    // Test create_families method
    let families = Individual::create_families(&individuals, &reference_date);
    assert_eq!(families.len(), 2, "Should create 2 family models");
}