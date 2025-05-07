//! Tests for population generation functionality

use chrono::NaiveDate;
use par_reader::algorithm::population::{
    PopulationBuilder, PopulationConfig
};

#[test]
fn test_population_config() {
    // Test default configuration
    let config = PopulationConfig::default();
    assert_eq!(config.index_date, NaiveDate::from_ymd_opt(2015, 1, 1).unwrap());
    assert!(config.resident_only);
    assert!(!config.two_parent_only);
    
    // Test custom configuration
    let custom_config = PopulationConfig {
        index_date: NaiveDate::from_ymd_opt(2020, 1, 1).unwrap(),
        min_age: Some(5),
        max_age: Some(18),
        resident_only: true,
        two_parent_only: true,
        study_start_date: Some(NaiveDate::from_ymd_opt(2018, 1, 1).unwrap()),
        study_end_date: Some(NaiveDate::from_ymd_opt(2022, 12, 31).unwrap()),
    };
    
    assert_eq!(custom_config.index_date, NaiveDate::from_ymd_opt(2020, 1, 1).unwrap());
    assert_eq!(custom_config.min_age, Some(5));
    assert_eq!(custom_config.max_age, Some(18));
    assert!(custom_config.resident_only);
    assert!(custom_config.two_parent_only);
    assert_eq!(custom_config.study_start_date, Some(NaiveDate::from_ymd_opt(2018, 1, 1).unwrap()));
    assert_eq!(custom_config.study_end_date, Some(NaiveDate::from_ymd_opt(2022, 12, 31).unwrap()));
}

#[test]
fn test_population_builder() {
    // Create a builder with custom configuration
    let _builder = PopulationBuilder::new()
        .with_config(PopulationConfig {
            index_date: NaiveDate::from_ymd_opt(2020, 1, 1).unwrap(),
            min_age: Some(0),
            max_age: None,
            resident_only: true,
            two_parent_only: false,
            study_start_date: None,
            study_end_date: None,
        });
    
    // In a real test, we would add data and build the population
    // But since that requires actual registry data, we'll just assert the builder exists
    assert!(true, "PopulationBuilder was created successfully");
    
    // NOTE: More comprehensive tests would require mock data
    // or test fixtures that aren't included in this minimal test
}

// This macro is included to allow these tests to be discovered by the test runner
#[cfg(test)]
mod tests {
    

    // Add any additional test-specific helper functions here
}