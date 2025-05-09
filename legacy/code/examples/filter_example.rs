//! Example of using the generic filter framework
//!
//! This file demonstrates how to use the generic filter framework
//! to create and combine filters for various types.

use std::collections::HashSet;

use crate::error::{ParquetReaderError, Result};
use crate::filter::generic::*;
use crate::models::Individual;

fn main() -> Result<()> {
    println!("Generic Filter Framework Example");
    println!("================================");

    // Example with simple integer filtering
    println!("\nInteger Filtering:");
    filter_integers_example()?;

    // Example with domain entities (Individuals)
    println!("\nIndividual Filtering:");
    filter_individuals_example()?;

    // Example with filter composition
    println!("\nFilter Composition:");
    filter_composition_example()?;

    // Example with filter builder
    println!("\nFilter Builder:");
    filter_builder_example()?;

    // Example with filter extension trait
    println!("\nFilter Extension Trait:");
    filter_extension_example()?;

    Ok(())
}

// A simple integer filter that only accepts even numbers
#[derive(Debug, Clone)]
struct EvenNumberFilter;

impl Filter<i32> for EvenNumberFilter {
    fn apply(&self, input: &i32) -> Result<i32> {
        if input % 2 == 0 {
            Ok(*input)
        } else {
            Err(ParquetReaderError::FilterExcluded {
                message: format!("Number {} is not even", input),
            }
            .into())
        }
    }

    fn required_resources(&self) -> HashSet<String> {
        HashSet::new()
    }
}

// A simple integer filter that only accepts numbers greater than a threshold
#[derive(Debug, Clone)]
struct GreaterThanFilter {
    threshold: i32,
}

impl Filter<i32> for GreaterThanFilter {
    fn apply(&self, input: &i32) -> Result<i32> {
        if *input > self.threshold {
            Ok(*input)
        } else {
            Err(ParquetReaderError::FilterExcluded {
                message: format!("Number {} is not greater than {}", input, self.threshold),
            }
            .into())
        }
    }

    fn required_resources(&self) -> HashSet<String> {
        HashSet::new()
    }
}

// Example of using filters with integers
fn filter_integers_example() -> Result<()> {
    let numbers = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    println!("Input numbers: {:?}", numbers);

    // Create filters
    let even_filter = EvenNumberFilter;
    let greater_than_five = GreaterThanFilter { threshold: 5 };

    // Apply filters
    let mut even_numbers = Vec::new();
    let mut numbers_greater_than_five = Vec::new();

    for num in &numbers {
        match even_filter.apply(num) {
            Ok(n) => even_numbers.push(n),
            Err(_) => {}
        }

        match greater_than_five.apply(num) {
            Ok(n) => numbers_greater_than_five.push(n),
            Err(_) => {}
        }
    }

    println!("Even numbers: {:?}", even_numbers);
    println!("Numbers greater than 5: {:?}", numbers_greater_than_five);

    Ok(())
}

// A filter for individuals under a certain age
#[derive(Debug, Clone)]
struct AgeFilter {
    max_age: i32,
}

impl Filter<Individual> for AgeFilter {
    fn apply(&self, input: &Individual) -> Result<Individual> {
        let age = input.age.unwrap_or(0);
        if age <= self.max_age {
            Ok(input.clone())
        } else {
            Err(ParquetReaderError::FilterExcluded {
                message: format!("Individual with age {} exceeds maximum age {}", age, self.max_age),
            }
            .into())
        }
    }

    fn required_resources(&self) -> HashSet<String> {
        let mut resources = HashSet::new();
        resources.insert("age".to_string());
        resources
    }
}

// A filter for individuals of a specific gender
#[derive(Debug, Clone)]
struct GenderFilter {
    gender: String,
}

impl Filter<Individual> for GenderFilter {
    fn apply(&self, input: &Individual) -> Result<Individual> {
        if let Some(gender) = &input.gender {
            if gender == &self.gender {
                Ok(input.clone())
            } else {
                Err(ParquetReaderError::FilterExcluded {
                    message: format!(
                        "Individual with gender {} doesn't match required gender {}",
                        gender, self.gender
                    ),
                }
                .into())
            }
        } else {
            Err(ParquetReaderError::FilterExcluded {
                message: "Individual has no gender".to_string(),
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

// Example of using filters with domain entities
fn filter_individuals_example() -> Result<()> {
    // Create some test individuals
    let individuals = vec![
        Individual {
            pnr: "12345".to_string(),
            age: Some(25),
            gender: Some("F".to_string()),
            ..Default::default()
        },
        Individual {
            pnr: "67890".to_string(),
            age: Some(35),
            gender: Some("M".to_string()),
            ..Default::default()
        },
        Individual {
            pnr: "24680".to_string(),
            age: Some(18),
            gender: Some("F".to_string()),
            ..Default::default()
        },
    ];

    // Create filters
    let young_filter = AgeFilter { max_age: 30 };
    let female_filter = GenderFilter {
        gender: "F".to_string(),
    };

    // Apply filters
    let mut young_individuals = Vec::new();
    let mut female_individuals = Vec::new();

    for individual in &individuals {
        match young_filter.apply(individual) {
            Ok(i) => young_individuals.push(i),
            Err(_) => {}
        }

        match female_filter.apply(individual) {
            Ok(i) => female_individuals.push(i),
            Err(_) => {}
        }
    }

    println!(
        "Young individuals (age <= 30): {}",
        young_individuals.len()
    );
    for individual in &young_individuals {
        println!("  - PNR: {}, Age: {}", individual.pnr, individual.age.unwrap_or(0));
    }

    println!("Female individuals: {}", female_individuals.len());
    for individual in &female_individuals {
        println!(
            "  - PNR: {}, Gender: {}",
            individual.pnr,
            individual.gender.as_ref().unwrap()
        );
    }

    Ok(())
}

// Example of composing filters using AND, OR, and NOT
fn filter_composition_example() -> Result<()> {
    let numbers = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    println!("Input numbers: {:?}", numbers);

    // Create base filters
    let even_filter = EvenNumberFilter;
    let greater_than_five = GreaterThanFilter { threshold: 5 };

    // Create composite filters
    let and_filter = AndFilter::new(vec![even_filter.clone(), greater_than_five.clone()]);
    let or_filter = OrFilter::new(vec![even_filter.clone(), greater_than_five.clone()]);
    let not_even_filter = NotFilter::new(even_filter.clone());

    // Apply composite filters
    println!("AND Filter (even AND > 5):");
    for num in &numbers {
        match and_filter.apply(num) {
            Ok(n) => println!("  - {} passed", n),
            Err(_) => {}
        }
    }

    println!("OR Filter (even OR > 5):");
    for num in &numbers {
        match or_filter.apply(num) {
            Ok(n) => println!("  - {} passed", n),
            Err(_) => {}
        }
    }

    println!("NOT Filter (not even):");
    for num in &numbers {
        match not_even_filter.apply(num) {
            Ok(n) => println!("  - {} passed", n),
            Err(_) => {}
        }
    }

    Ok(())
}

// Example of using the filter builder pattern
fn filter_builder_example() -> Result<()> {
    let numbers = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    println!("Input numbers: {:?}", numbers);

    // Create base filters
    let even_filter = EvenNumberFilter;
    let greater_than_five = GreaterThanFilter { threshold: 5 };
    let greater_than_eight = GreaterThanFilter { threshold: 8 };

    // Use filter builder to create a complex filter
    let complex_and_filter = FilterBuilder::new()
        .add_filter(even_filter.clone())
        .add_filter(greater_than_five.clone())
        .build_and();

    let complex_or_filter = FilterBuilder::new()
        .add_filter(even_filter)
        .add_filter(greater_than_eight)
        .build_or();

    // Apply the filters
    println!("Complex AND Filter (even AND > 5):");
    for num in &numbers {
        match complex_and_filter.apply(num) {
            Ok(n) => println!("  - {} passed", n),
            Err(_) => {}
        }
    }

    println!("Complex OR Filter (even OR > 8):");
    for num in &numbers {
        match complex_or_filter.apply(num) {
            Ok(n) => println!("  - {} passed", n),
            Err(_) => {}
        }
    }

    Ok(())
}

// Example of using the filter extension trait
fn filter_extension_example() -> Result<()> {
    let numbers = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    println!("Input numbers: {:?}", numbers);

    // Create base filters
    let even_filter = EvenNumberFilter;
    let greater_than_five = GreaterThanFilter { threshold: 5 };

    // Use extension methods to combine filters
    let extended_and_filter = even_filter.clone().and(greater_than_five.clone());
    let extended_or_filter = even_filter.clone().or(greater_than_five.clone());
    let extended_not_filter = even_filter.clone().not();

    // Apply the filters
    println!("Extension AND Filter (even AND > 5):");
    for num in &numbers {
        match extended_and_filter.apply(num) {
            Ok(n) => println!("  - {} passed", n),
            Err(_) => {}
        }
    }

    println!("Extension OR Filter (even OR > 5):");
    for num in &numbers {
        match extended_or_filter.apply(num) {
            Ok(n) => println!("  - {} passed", n),
            Err(_) => {}
        }
    }

    println!("Extension NOT Filter (not even):");
    for num in &numbers {
        match extended_not_filter.apply(num) {
            Ok(n) => println!("  - {} passed", n),
            Err(_) => {}
        }
    }

    Ok(())
}