//! Example of adapting matching criteria to the generic filter framework
//!
//! This file demonstrates how to adapt the matching criteria from
//! the algorithm module to use the generic Filter trait.

use chrono::NaiveDate;
use std::collections::HashSet;

use crate::algorithm::matching::criteria::MatchingCriteria;
use crate::error::{ParquetReaderError, Result};
use crate::filter::generic::*;
use crate::models::Individual;

fn main() -> Result<()> {
    println!("Matching Criteria Filter Example");
    println!("================================");

    // Create sample individuals
    let individuals = create_sample_individuals();

    // Create matching criteria
    let criteria = MatchingCriteria {
        birth_date_window_days: 30,
        parent_birth_date_window_days: 365,
        require_both_parents: false,
        require_same_gender: true,
        match_family_size: true,
        family_size_tolerance: 1,
        match_education_level: true,
        match_geography: true,
        match_parental_status: true,
        match_immigrant_background: false,
    };

    // Create a reference individual to match against
    let reference_individual = &individuals[0];

    // Create the matching criteria filter
    let filter = MatchingCriteriaFilter::new(criteria, reference_individual.clone());

    // Apply the filter to all individuals
    println!("Applying matching criteria filter:");
    for individual in &individuals {
        match filter.apply(individual) {
            Ok(_) => println!("  - {} matches criteria", individual.pnr),
            Err(_) => println!("  - {} does not match criteria", individual.pnr),
        }
    }

    // Demonstrate using the filter with the extension trait
    let gender_filter = GenderFilter {
        gender: crate::models::individual::Gender::Female,
    };

    // Create a composite filter using extension methods
    let composite_filter = filter.and(gender_filter);

    // Apply the composite filter
    println!("\nApplying composite matching + gender filter:");
    for individual in &individuals {
        match composite_filter.apply(individual) {
            Ok(_) => println!("  - {} matches composite criteria", individual.pnr),
            Err(_) => println!("  - {} does not match composite criteria", individual.pnr),
        }
    }

    // Demonstrate factory registration
    println!("\nDemonstrating filter factory:");
    let factory = FilterRegistry::new();
    let factory = factory
        .register::<MatchingCriteriaFilter<Individual>>("matching")
        .register::<GenderFilter>("gender")
        .register::<AgeRangeFilter>("age");

    // Use the factory
    let params: serde_json::Value = serde_json::json!({
        "reference_individual": {
            "pnr": "12345",
            "gender": "Male",
            "age": 30
        },
        "criteria": {
            "birth_date_window_days": 30,
            "require_same_gender": true
        }
    });

    let filter = factory.create_filter::<Individual>("matching", &params)?;

    println!("Created filter from factory: {:?}", filter);

    Ok(())
}

/// A filter that implements matching criteria for individuals
#[derive(Debug, Clone)]
struct MatchingCriteriaFilter<T> {
    criteria: MatchingCriteria,
    reference: T,
}

impl<T: Clone> MatchingCriteriaFilter<T> {
    /// Create a new matching criteria filter
    fn new(criteria: MatchingCriteria, reference: T) -> Self {
        Self {
            criteria,
            reference,
        }
    }
}

/// Implementation for Individual matching
impl Filter<Individual> for MatchingCriteriaFilter<Individual> {
    fn apply(&self, input: &Individual) -> Result<Individual> {
        // Check gender matching if required
        if self.criteria.require_same_gender {
            if input.gender != self.reference.gender {
                return Err(ParquetReaderError::FilterExcluded {
                    message: format!(
                        "Gender mismatch: {:?} vs {:?}",
                        input.gender, self.reference.gender
                    ),
                }
                .into());
            }
        }

        // Check birth date matching
        if let (Some(input_birth), Some(ref_birth)) = (input.birthdate, self.reference.birthdate) {
            if !self.criteria.is_birth_date_match(ref_birth, input_birth) {
                return Err(ParquetReaderError::FilterExcluded {
                    message: format!(
                        "Birth date outside window: {} vs {}",
                        input_birth, ref_birth
                    ),
                }
                .into());
            }
        }

        // Check education level matching if required
        if self.criteria.match_education_level {
            if input.education_level != self.reference.education_level {
                return Err(ParquetReaderError::FilterExcluded {
                    message: format!(
                        "Education level mismatch: {:?} vs {:?}",
                        input.education_level, self.reference.education_level
                    ),
                }
                .into());
            }
        }

        // Check geography matching if required
        if self.criteria.match_geography {
            if input.municipality_code != self.reference.municipality_code {
                return Err(ParquetReaderError::FilterExcluded {
                    message: "Municipality mismatch".to_string(),
                }
                .into());
            }
        }

        // All criteria matched
        Ok(input.clone())
    }

    fn required_resources(&self) -> HashSet<String> {
        let mut resources = HashSet::new();

        // Always need PNR
        resources.insert("pnr".to_string());

        // Conditionally add other resources
        if self.criteria.require_same_gender {
            resources.insert("gender".to_string());
        }

        // Birth date matching
        resources.insert("birthdate".to_string());

        // Education level matching
        if self.criteria.match_education_level {
            resources.insert("education_level".to_string());
        }

        // Geography matching
        if self.criteria.match_geography {
            resources.insert("municipality_code".to_string());
        }

        resources
    }
}

/// A gender filter for individuals
#[derive(Debug, Clone)]
struct GenderFilter {
    gender: crate::models::individual::Gender,
}

impl Filter<Individual> for GenderFilter {
    fn apply(&self, input: &Individual) -> Result<Individual> {
        if input.gender == self.gender {
            Ok(input.clone())
        } else {
            Err(ParquetReaderError::FilterExcluded {
                message: format!(
                    "Individual gender {:?} doesn't match required gender {:?}",
                    input.gender, self.gender
                ),
            }
            .into())
        }
    }

    fn required_resources(&self) -> HashSet<String> {
        let mut resources = HashSet::new();
        resources.insert("gender".to_string());
        resources
    }
}

/// An age range filter for individuals
#[derive(Debug, Clone)]
struct AgeRangeFilter {
    min_age: u32,
    max_age: u32,
    reference_date: NaiveDate,
}

impl Filter<Individual> for AgeRangeFilter {
    fn apply(&self, input: &Individual) -> Result<Individual> {
        if let Some(age) = input.age_at(&self.reference_date) {
            let age_u32 = age as u32;

            if age_u32 >= self.min_age && age_u32 <= self.max_age {
                Ok(input.clone())
            } else {
                Err(ParquetReaderError::FilterExcluded {
                    message: format!(
                        "Individual age {} is outside the range {}-{}",
                        age_u32, self.min_age, self.max_age
                    ),
                }
                .into())
            }
        } else {
            Err(ParquetReaderError::FilterExcluded {
                message: "Individual has no calculable age".to_string(),
            }
            .into())
        }
    }

    fn required_resources(&self) -> HashSet<String> {
        let mut resources = HashSet::new();
        resources.insert("birthdate".to_string());
        resources
    }
}

/// A registry for creating filters by name
#[derive(Debug, Default)]
struct FilterRegistry {
    // Map of filter name to factory function
    factories: std::collections::HashMap<
        String,
        Box<dyn Fn(&serde_json::Value) -> Result<Box<dyn std::any::Any>> + Send + Sync>,
    >,
}

impl FilterRegistry {
    /// Create a new filter registry
    fn new() -> Self {
        Self {
            factories: std::collections::HashMap::new(),
        }
    }

    /// Register a filter type with the registry
    fn register<F>(mut self, name: &str) -> Self
    where
        F: 'static + Send + Sync + std::any::Any + serde::de::DeserializeOwned,
    {
        let factory = move |params: &serde_json::Value| -> Result<Box<dyn std::any::Any>> {
            let filter = serde_json::from_value::<F>(params.clone())
                .map_err(|e| anyhow::anyhow!("Failed to deserialize filter: {}", e))?;

            Ok(Box::new(filter))
        };

        self.factories.insert(name.to_string(), Box::new(factory));
        self
    }

    /// Create a filter from a name and parameters
    fn create_filter<T>(
        &self,
        name: &str,
        params: &serde_json::Value,
    ) -> Result<Box<dyn Filter<T> + Send + Sync>>
    where
        T: 'static + Clone + std::fmt::Debug + Send + Sync,
    {
        let factory = self
            .factories
            .get(name)
            .ok_or_else(|| anyhow::anyhow!("No filter factory registered for name: {}", name))?;

        let filter = factory(params)?;

        let filter = filter
            .downcast::<Box<dyn Filter<T> + Send + Sync>>()
            .map_err(|_| anyhow::anyhow!("Failed to downcast filter to correct type"))?;

        Ok(*filter)
    }
}

// Helper function to create sample individuals for testing
fn create_sample_individuals() -> Vec<Individual> {
    vec![
        Individual {
            pnr: "12345".to_string(),
            birthdate: Some(NaiveDate::from_ymd_opt(1980, 1, 1).unwrap()),
            gender: crate::models::individual::Gender::Male,
            age: Some(43),
            is_rural: true,
            education_level: crate::models::individual::EducationLevel::Higher,
            municipality_code: Some("101".to_string()),
            ..Default::default()
        },
        Individual {
            pnr: "23456".to_string(),
            birthdate: Some(NaiveDate::from_ymd_opt(1980, 1, 15).unwrap()), // Close to first
            gender: crate::models::individual::Gender::Male,                // Same gender
            age: Some(43),
            is_rural: false,
            education_level: crate::models::individual::EducationLevel::Higher, // Same education
            municipality_code: Some("101".to_string()),                         // Same municipality
            ..Default::default()
        },
        Individual {
            pnr: "34567".to_string(),
            birthdate: Some(NaiveDate::from_ymd_opt(2010, 3, 10).unwrap()), // Different birth year
            gender: crate::models::individual::Gender::Male,                // Same gender
            age: Some(13),
            is_rural: false,
            education_level: crate::models::individual::EducationLevel::Primary, // Different education
            municipality_code: Some("102".to_string()), // Different municipality
            ..Default::default()
        },
        Individual {
            pnr: "45678".to_string(),
            birthdate: Some(NaiveDate::from_ymd_opt(1980, 1, 5).unwrap()), // Close to first
            gender: crate::models::individual::Gender::Female,             // Different gender
            age: Some(43),
            is_rural: true,
            education_level: crate::models::individual::EducationLevel::Higher, // Same education
            municipality_code: Some("101".to_string()),                         // Same municipality
            ..Default::default()
        },
    ]
}
