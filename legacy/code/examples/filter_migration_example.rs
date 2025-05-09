//! Example of migrating from legacy filtering to the generic filter framework
//!
//! This file demonstrates how to migrate from the legacy FilterCriteria-based
//! filtering system to the new generic Filter trait-based system.

use chrono::NaiveDate;
use std::collections::HashSet;

use crate::error::{ParquetReaderError, Result};
use crate::filter::generic::*;
use crate::models::{Child, Family, Individual, family::FamilyType};

// Import both the old and new filter systems
use crate::algorithm::population::filters::{
    FilterCriteria, IndividualFilter as LegacyIndividualFilter,
};
use crate::filter::{Filter, FilterBuilder, FilterExt};

fn main() -> Result<()> {
    println!("Filter Migration Example");
    println!("=======================");

    // Example migrating individual filters
    println!("\nMigrating Individual Filters:");
    migrate_individual_filters_example()?;

    // Example migrating family filters
    println!("\nMigrating Family Filters:");
    migrate_family_filters_example()?;

    // Example of adapter approach for backwards compatibility
    println!("\nAdapter Approach for Backwards Compatibility:");
    adapter_approach_example()?;

    // Example of using both systems together during migration
    println!("\nGradual Migration Approach:");
    gradual_migration_example()?;

    Ok(())
}

// Example showing migration of individual filters
fn migrate_individual_filters_example() -> Result<()> {
    println!("Before: Legacy IndividualFilter with FilterCriteria trait");

    // Create sample individuals
    let individuals = create_sample_individuals();

    // Create a legacy filter
    let legacy_age_filter = LegacyIndividualFilter::AgeRange {
        min_age: Some(18),
        max_age: Some(65),
        reference_date: NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(),
    };

    // Use the legacy filter
    let filtered_individuals = individuals
        .iter()
        .filter(|i| legacy_age_filter.meets_criteria(i))
        .count();

    println!(
        "  - Legacy filter matched {} individuals",
        filtered_individuals
    );

    println!("\nAfter: Generic Filter<Individual> implementation");

    // Create equivalent filter with the new generic approach
    let age_filter = AgeRangeFilter {
        min_age: 18,
        max_age: 65,
        reference_date: NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(),
    };

    // Use the new filter
    let mut filtered_count = 0;
    for individual in &individuals {
        match age_filter.apply(individual) {
            Ok(_) => filtered_count += 1,
            Err(_) => {}
        }
    }

    println!("  - Generic filter matched {} individuals", filtered_count);

    // Example of combining filters with the legacy approach
    let legacy_combined_filter = LegacyIndividualFilter::All(vec![
        legacy_age_filter,
        LegacyIndividualFilter::Gender(crate::models::individual::Gender::Female),
    ]);

    // Use the legacy combined filter
    let filtered_individuals = individuals
        .iter()
        .filter(|i| legacy_combined_filter.meets_criteria(i))
        .count();

    println!(
        "\nLegacy combined filter matched {} individuals",
        filtered_individuals
    );

    // Create equivalent filter with the new generic approach
    let gender_filter = GenderFilter {
        gender: crate::models::individual::Gender::Female,
    };

    // Create a combined filter using the FilterBuilder
    let combined_filter = FilterBuilder::new()
        .add_filter(age_filter)
        .add_filter(gender_filter)
        .build_and();

    // Use the new combined filter
    let mut filtered_count = 0;
    for individual in &individuals {
        match combined_filter.apply(individual) {
            Ok(_) => filtered_count += 1,
            Err(_) => {}
        }
    }

    println!(
        "Generic combined filter matched {} individuals",
        filtered_count
    );

    // Alternative approach using extension methods
    let combined_filter_ext = age_filter.and(gender_filter);

    // Use the extension method filter
    let mut filtered_count = 0;
    for individual in &individuals {
        match combined_filter_ext.apply(individual) {
            Ok(_) => filtered_count += 1,
            Err(_) => {}
        }
    }

    println!(
        "Generic filter with extension methods matched {} individuals",
        filtered_count
    );

    // Example of chaining multiple filters with extension methods
    let rural_filter = RuralAreaFilter { is_rural: true };
    let complex_filter = age_filter.and(gender_filter).or(rural_filter);

    // Use the chained filter
    let mut filtered_count = 0;
    for individual in &individuals {
        match complex_filter.apply(individual) {
            Ok(_) => filtered_count += 1,
            Err(_) => {}
        }
    }

    println!(
        "Chained filter (age AND gender) OR rural matched {} individuals",
        filtered_count
    );

    Ok(())
}

// Example showing migration of family filters
fn migrate_family_filters_example() -> Result<()> {
    println!("Migrating from legacy FamilyFilter to generic Filter<Family>");

    // Create sample families
    let families = create_sample_families();

    // Legacy filter
    let legacy_family_filter = crate::algorithm::population::filters::FamilyFilter::FamilySize {
        min_children: Some(2),
        max_children: Some(5),
    };

    // Use the legacy filter
    let filtered_families = families
        .iter()
        .filter(|f| legacy_family_filter.meets_criteria(f))
        .count();

    println!("  - Legacy filter matched {} families", filtered_families);

    // Create equivalent filter with the new generic approach
    let family_size_filter = FamilySizeFilter {
        min_children: Some(2),
        max_children: Some(5),
    };

    // Use the new filter
    let mut filtered_count = 0;
    for family in &families {
        match family_size_filter.apply(family) {
            Ok(_) => filtered_count += 1,
            Err(_) => {}
        }
    }

    println!("  - Generic filter matched {} families", filtered_count);

    Ok(())
}

// Example of using an adapter approach for backwards compatibility
fn adapter_approach_example() -> Result<()> {
    println!("Using adapters to maintain backward compatibility");

    // Create sample individuals
    let individuals = create_sample_individuals();

    // Legacy filter
    let legacy_filter = LegacyIndividualFilter::AgeRange {
        min_age: Some(18),
        max_age: Some(65),
        reference_date: NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(),
    };

    // Create an adapter that converts the legacy filter to the new system
    let adapter = LegacyFilterAdapter::new(legacy_filter);

    // Use the adapter as a generic filter
    let mut filtered_count = 0;
    for individual in &individuals {
        match adapter.apply(individual) {
            Ok(_) => filtered_count += 1,
            Err(_) => {}
        }
    }

    println!(
        "  - Adapter approach matched {} individuals",
        filtered_count
    );

    Ok(())
}

// Example of gradual migration path
fn gradual_migration_example() -> Result<()> {
    println!("Demonstrating gradual migration path");

    // Create a combined filter that uses both legacy and new filter systems
    let individuals = create_sample_individuals();

    // Legacy part (for filters not yet migrated)
    let legacy_filter = LegacyIndividualFilter::Gender(crate::models::individual::Gender::Female);
    let legacy_adapter = LegacyFilterAdapter::new(legacy_filter);

    // New part (for already migrated filters)
    let new_filter = AgeRangeFilter {
        min_age: 18,
        max_age: 65,
        reference_date: NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(),
    };

    // Combine using the new framework
    let combined_filter = legacy_adapter.and(new_filter);

    // Apply the combined filter
    let mut filtered_count = 0;
    for individual in &individuals {
        match combined_filter.apply(individual) {
            Ok(_) => filtered_count += 1,
            Err(_) => {}
        }
    }

    println!(
        "  - Combined old+new approach matched {} individuals",
        filtered_count
    );

    Ok(())
}

// Example implementation of AgeRangeFilter using the new Filter trait
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

// Example implementation of GenderFilter using the new Filter trait
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

// Example implementation of RuralAreaFilter using the new Filter trait
#[derive(Debug, Clone)]
struct RuralAreaFilter {
    is_rural: bool,
}

impl Filter<Individual> for RuralAreaFilter {
    fn apply(&self, input: &Individual) -> Result<Individual> {
        if input.is_rural == self.is_rural {
            Ok(input.clone())
        } else {
            Err(ParquetReaderError::FilterExcluded {
                message: format!(
                    "Individual rural status {} doesn't match required status {}",
                    input.is_rural, self.is_rural
                ),
            }
            .into())
        }
    }

    fn required_resources(&self) -> HashSet<String> {
        let mut resources = HashSet::new();
        resources.insert("is_rural".to_string());
        resources
    }
}

// Example implementation of FamilySizeFilter using the new Filter trait
#[derive(Debug, Clone)]
struct FamilySizeFilter {
    min_children: Option<usize>,
    max_children: Option<usize>,
}

impl Filter<Family> for FamilySizeFilter {
    fn apply(&self, input: &Family) -> Result<Family> {
        let size = input.family_size();

        // Check minimum size constraint if specified
        if let Some(min) = self.min_children {
            if size < min {
                return Err(ParquetReaderError::FilterExcluded {
                    message: format!("Family size {} is less than minimum {}", size, min),
                }
                .into());
            }
        }

        // Check maximum size constraint if specified
        if let Some(max) = self.max_children {
            if size > max {
                return Err(ParquetReaderError::FilterExcluded {
                    message: format!("Family size {} is greater than maximum {}", size, max),
                }
                .into());
            }
        }

        Ok(input.clone())
    }

    fn required_resources(&self) -> HashSet<String> {
        let mut resources = HashSet::new();
        resources.insert("children".to_string());
        resources
    }
}

// Adapter to make legacy filters compatible with the new Filter trait
#[derive(Debug, Clone)]
struct LegacyFilterAdapter<T, F>
where
    F: FilterCriteria<T> + std::fmt::Debug + Clone,
    T: Clone,
{
    legacy_filter: F,
    _phantom: std::marker::PhantomData<T>,
}

impl<T, F> LegacyFilterAdapter<T, F>
where
    F: FilterCriteria<T> + std::fmt::Debug + Clone,
    T: Clone,
{
    fn new(legacy_filter: F) -> Self {
        Self {
            legacy_filter,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T, F> Filter<T> for LegacyFilterAdapter<T, F>
where
    F: FilterCriteria<T> + std::fmt::Debug + Clone,
    T: Clone,
{
    fn apply(&self, input: &T) -> Result<T> {
        if self.legacy_filter.meets_criteria(input) {
            Ok(input.clone())
        } else {
            Err(ParquetReaderError::FilterExcluded {
                message: "Entity excluded by legacy filter".to_string(),
            }
            .into())
        }
    }

    fn required_resources(&self) -> HashSet<String> {
        // Legacy filters don't have a way to express required resources
        HashSet::new()
    }
}

// Helper function to create sample individuals for testing
fn create_sample_individuals() -> Vec<Individual> {
    vec![
        Individual {
            pnr: "12345".to_string(),
            birth_date: Some(NaiveDate::from_ymd_opt(1980, 1, 1).unwrap()),
            gender: crate::models::individual::Gender::Male,
            age: Some(43),
            is_rural: true,
            ..Default::default()
        },
        Individual {
            pnr: "23456".to_string(),
            birth_date: Some(NaiveDate::from_ymd_opt(1990, 6, 15).unwrap()),
            gender: crate::models::individual::Gender::Female,
            age: Some(33),
            is_rural: false,
            ..Default::default()
        },
        Individual {
            pnr: "34567".to_string(),
            birth_date: Some(NaiveDate::from_ymd_opt(2010, 3, 10).unwrap()),
            gender: crate::models::individual::Gender::Male,
            age: Some(13),
            is_rural: false,
            ..Default::default()
        },
        Individual {
            pnr: "45678".to_string(),
            birth_date: Some(NaiveDate::from_ymd_opt(1995, 9, 20).unwrap()),
            gender: crate::models::individual::Gender::Female,
            age: Some(28),
            is_rural: true,
            ..Default::default()
        },
    ]
}

// Helper function to create sample families for testing
fn create_sample_families() -> Vec<Family> {
    vec![
        Family {
            family_id: "family1".to_string(),
            family_type: FamilyType::NuclearFamily,
            is_rural: true,
            has_parental_comorbidity: false,
            has_support_network: true,
            children: vec![
                // 1 child
                Child {
                    pnr: "child1".to_string(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        },
        Family {
            family_id: "family2".to_string(),
            family_type: FamilyType::SingleParent,
            is_rural: false,
            has_parental_comorbidity: true,
            has_support_network: false,
            children: vec![
                // 3 children
                cChild {
                    pnr: "child2".to_string(),
                    ..Default::default()
                },
                Child {
                    pnr: "child3".to_string(),
                    ..Default::default()
                },
                Child {
                    pnr: "child4".to_string(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        },
        Family {
            family_id: "family3".to_string(),
            family_type: FamilyType::NuclearFamily,
            is_rural: true,
            has_parental_comorbidity: false,
            has_support_network: true,
            children: vec![
                // 6 children
                Child {
                    pnr: "child5".to_string(),
                    ..Default::default()
                },
                Child {
                    pnr: "child6".to_string(),
                    ..Default::default()
                },
                Child {
                    pnr: "child7".to_string(),
                    ..Default::default()
                },
                Child {
                    pnr: "child8".to_string(),
                    ..Default::default()
                },
                Child {
                    pnr: "child9".to_string(),
                    ..Default::default()
                },
                Child {
                    pnr: "child10".to_string(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        },
    ]
}
