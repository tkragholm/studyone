use crate::utils::{families, individuals};
use par_reader::error::{ParquetReaderError, Result};
use par_reader::filter::generic::*;
use par_reader::models::{Family, Individual, individual::Gender};
use std::collections::HashSet;

/// Test basic includes and excludes filter
#[test]
fn test_basic_filters() -> () {
    // Test data
    let test_value = 42;

    // Include all filter
    let include_filter = IncludeAllFilter;
    // The apply method doesn't take generic parameters
    assert!(include_filter.apply(&test_value).is_ok());
    assert_eq!(
        <IncludeAllFilter as Filter<i32>>::required_resources(&include_filter).len(),
        0
    );

    // Exclude all filter
    let exclude_filter = ExcludeAllFilter;
    // The apply method doesn't take generic parameters
    assert!(exclude_filter.apply(&test_value).is_err());
    assert_eq!(
        <ExcludeAllFilter as Filter<i32>>::required_resources(&exclude_filter).len(),
        0
    );
}

/// Test simple filters with AND, OR, NOT operations
#[test]
fn test_filter_combinators() -> () {
    // Create simple filters
    let even_filter = EvenNumberFilter;
    let gt_five_filter = GreaterThanFilter { threshold: 5 };

    // Test data
    let values = [2, 4, 5, 7, 8, 10];

    // Test AND filter using BoxedFilter to handle heterogeneous filter types
    let boxed_even = BoxedFilter::new(even_filter.clone());
    let boxed_gt_five = BoxedFilter::new(gt_five_filter);

    let and_filter = AndFilter::new(vec![boxed_even.clone(), boxed_gt_five.clone()]);
    let and_results: Vec<i32> = values
        .iter()
        .filter_map(|v| and_filter.apply(v).ok())
        .collect();
    assert_eq!(and_results, vec![8, 10]);

    // Test OR filter using BoxedFilter to handle heterogeneous filter types
    let or_filter = OrFilter::new(vec![boxed_even, boxed_gt_five]);
    let or_results: Vec<i32> = values
        .iter()
        .filter_map(|v| or_filter.apply(v).ok())
        .collect();
    assert_eq!(or_results, vec![2, 4, 5, 7, 8, 10]);

    // Test NOT filter
    let not_filter = NotFilter::new(even_filter);
    let not_results: Vec<i32> = values
        .iter()
        .filter_map(|v| not_filter.apply(v).ok())
        .collect();
    assert_eq!(not_results, vec![5, 7]);
}

/// Test filter builder pattern
#[test]
fn test_filter_builder() -> () {
    // Create simple filters
    let even_filter = EvenNumberFilter;
    let gt_five_filter = GreaterThanFilter { threshold: 5 };
    let gt_eight_filter = GreaterThanFilter { threshold: 8 };

    // Test data
    let values = [2, 4, 5, 7, 8, 10, 12];

    // Test building an AND filter using BoxedFilter for consistent types
    let boxed_even = BoxedFilter::new(even_filter);
    let boxed_gt_five = BoxedFilter::new(gt_five_filter);
    let boxed_gt_eight = BoxedFilter::new(gt_eight_filter.clone());

    let and_filter = FilterBuilder::new()
        .add_filter(boxed_even.clone())
        .add_filter(boxed_gt_five.clone())
        .build_and();

    let and_results: Vec<i32> = values
        .iter()
        .filter_map(|v| and_filter.apply(v).ok())
        .collect();
    assert_eq!(and_results, vec![8, 10, 12]);

    // Test building an OR filter using BoxedFilter for consistent types
    let or_filter = FilterBuilder::new()
        .add_filter(boxed_even)
        .add_filter(boxed_gt_eight)
        .build_or();

    let or_results: Vec<i32> = values
        .iter()
        .filter_map(|v| or_filter.apply(v).ok())
        .collect();
    assert_eq!(or_results, vec![2, 4, 8, 10, 12]);

    // Test building a NOT-AND filter using BoxedFilter for consistent types
    let not_and_filter = FilterBuilder::new()
        .add_filter(boxed_gt_five)
        .add_filter(BoxedFilter::new(gt_eight_filter))
        .build_not_and();

    let not_and_results: Vec<i32> = values
        .iter()
        .filter_map(|v| not_and_filter.apply(v).ok())
        .collect();
    assert_eq!(not_and_results, vec![2, 4, 5, 7]);
}

/// Test filter extension methods
#[test]
fn test_filter_extension_methods() -> () {
    // Create simple filters
    let even_filter1 = EvenNumberFilter;
    let even_filter2 = EvenNumberFilter;
    let even_filter3 = EvenNumberFilter;
    let gt_five_filter = GreaterThanFilter { threshold: 5 };
    let gt_eight_filter = GreaterThanFilter { threshold: 8 };

    // Test data
    let values = [2, 4, 5, 7, 8, 10, 12];

    // Test AND extension method
    let and_filter = even_filter1.and(gt_five_filter.clone());
    let and_results: Vec<i32> = values
        .iter()
        .filter_map(|v| and_filter.apply(v).ok())
        .collect();
    assert_eq!(and_results, vec![8, 10, 12]);

    // Test OR extension method
    let or_filter = even_filter2.or(gt_eight_filter.clone());
    let or_results: Vec<i32> = values
        .iter()
        .filter_map(|v| or_filter.apply(v).ok())
        .collect();
    assert_eq!(or_results, vec![2, 4, 8, 10, 12]);

    // Test NOT extension method
    let not_filter = even_filter3.not();
    let not_results: Vec<i32> = values
        .iter()
        .filter_map(|v| not_filter.apply(v).ok())
        .collect();
    assert_eq!(not_results, vec![5, 7]);

    // Test chaining extension methods with a new filter instance
    let even_filter4 = EvenNumberFilter;
    let chained_filter = even_filter4.and(gt_five_filter).or(gt_eight_filter);
    let chained_results: Vec<i32> = values
        .iter()
        .filter_map(|v| chained_filter.apply(v).ok())
        .collect();
    assert_eq!(chained_results, vec![8, 10, 12]);
}

/// Test boxed filter implementation
#[test]
fn test_boxed_filter() -> () {
    // Create a simple filter
    let even_filter = EvenNumberFilter;

    // Convert to a boxed filter
    let boxed_filter = BoxedFilter::new(even_filter);

    // Test data
    let values = [2, 3, 4, 5, 6];

    // Apply the boxed filter
    let results: Vec<i32> = values
        .iter()
        .filter_map(|v| boxed_filter.apply(v).ok())
        .collect();

    assert_eq!(results, vec![2, 4, 6]);

    // Test cloning the boxed filter
    let cloned_filter = boxed_filter;
    let cloned_results: Vec<i32> = values
        .iter()
        .filter_map(|v| cloned_filter.apply(v).ok())
        .collect();

    assert_eq!(cloned_results, vec![2, 4, 6]);
}

/// Test resource tracking
#[test]
fn test_resource_tracking() -> () {
    // Create filters with different resource requirements
    let age_filter = AgeFilter { max_age: 30 };
    let gender_filter = GenderFilter {
        gender: Gender::Female,
    };

    // Check individual resource sets
    let age_resources = age_filter.required_resources();
    let gender_resources = gender_filter.required_resources();

    assert!(age_resources.contains("age"));
    assert!(gender_resources.contains("gender"));

    // Check combined resource sets using BoxedFilter for consistent types
    let boxed_age = BoxedFilter::new(age_filter);
    let boxed_gender = BoxedFilter::new(gender_filter);
    let and_filter = AndFilter::new(vec![boxed_age, boxed_gender]);
    let combined_resources = and_filter.required_resources();

    assert!(combined_resources.contains("age"));
    assert!(combined_resources.contains("gender"));
    assert_eq!(combined_resources.len(), 2);
}

/// Test filters with domain entities
#[test]
fn test_domain_entity_filters() -> () {
    // Create test individuals
    let individuals = individuals::create_test_individuals();

    // Create domain-specific filters
    let age_filter = AgeFilter { max_age: 30 };
    let gender_filter = GenderFilter {
        gender: Gender::Female,
    };

    // Apply to individuals
    let age_filtered: Vec<Individual> = individuals
        .iter()
        .filter_map(|i| age_filter.apply(i).ok())
        .collect();

    let gender_filtered: Vec<Individual> = individuals
        .iter()
        .filter_map(|i| gender_filter.apply(i).ok())
        .collect();

    // Verify results
    assert_eq!(age_filtered.len(), 2);
    assert_eq!(gender_filtered.len(), 2);

    // Test combined filters
    let combined_filter = age_filter.and(gender_filter);

    let combined_filtered: Vec<Individual> = individuals
        .iter()
        .filter_map(|i| combined_filter.apply(i).ok())
        .collect();

    assert_eq!(combined_filtered.len(), 1);
    assert_eq!(combined_filtered[0].pnr, "45678");
}

/// Test filters with family entities
#[test]
fn test_family_filters() -> () {
    // Create test families
    let families = families::create_test_families();

    // Create family-specific filters
    let size_filter = FamilySizeFilter {
        min_children: Some(2),
        max_children: Some(5),
    };

    let rural_filter = FamilyRuralFilter { is_rural: true };

    // Apply to families
    let size_filtered: Vec<Family> = families
        .iter()
        .filter_map(|f| size_filter.apply(f).ok())
        .collect();

    let rural_filtered: Vec<Family> = families
        .iter()
        .filter_map(|f| rural_filter.apply(f).ok())
        .collect();

    // Verify results
    assert_eq!(size_filtered.len(), 1);
    assert_eq!(rural_filtered.len(), 2);

    // Test combined filters
    let combined_filter = size_filter.and(rural_filter);

    let combined_filtered: Vec<Family> = families
        .iter()
        .filter_map(|f| combined_filter.apply(f).ok())
        .collect();

    assert_eq!(combined_filtered.len(), 0); // No family matches both criteria
}

/// Test adapter pattern
#[test]
fn test_filter_adapters() -> () {
    // Create test individuals
    let individuals = individuals::create_test_individuals();

    // Test the legacy filter adapter
    let gender_predicate = |i: &Individual| i.gender == Gender::Female;
    // Calculate age using reference date
    let reference_date = chrono::NaiveDate::from_ymd_opt(2023, 1, 1).unwrap();
    // Use 'move' to capture reference_date by value
    let age_predicate = move |i: &Individual| i.age_at(&reference_date).unwrap_or(0) <= 30;

    let gender_predicate_filter =
        PredicateFilter::new(gender_predicate, vec!["gender".to_string()]);
    let age_predicate_filter = PredicateFilter::new(age_predicate, vec!["age".to_string()]);

    // Apply the predicate filters
    let gender_filtered: Vec<Individual> = individuals
        .iter()
        .filter_map(|i| gender_predicate_filter.apply(i).ok())
        .collect();

    let age_filtered: Vec<Individual> = individuals
        .iter()
        .filter_map(|i| age_predicate_filter.apply(i).ok())
        .collect();

    // Verify results
    assert_eq!(gender_filtered.len(), 2);
    assert_eq!(age_filtered.len(), 2);

    // Combine using the extension trait
    let combined_filter = gender_predicate_filter.and(age_predicate_filter);

    let combined_filtered: Vec<Individual> = individuals
        .iter()
        .filter_map(|i| combined_filter.apply(i).ok())
        .collect();

    assert_eq!(combined_filtered.len(), 1);
    assert_eq!(combined_filtered[0].pnr, "45678");
}

// Helper functions and sample filters for testing

// A filter for even numbers
#[derive(Debug, Clone)]
struct EvenNumberFilter;

impl Filter<i32> for EvenNumberFilter {
    fn apply(&self, input: &i32) -> Result<i32> {
        if input % 2 == 0 {
            Ok(*input)
        } else {
            Err(ParquetReaderError::FilterExcluded {
                message: format!("Number {input} is not even"),
            }
            .into())
        }
    }

    fn required_resources(&self) -> HashSet<String> {
        HashSet::new()
    }
}

// A filter for numbers greater than a threshold
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

// A filter that uses a predicate function
#[derive(Clone)]
struct PredicateFilter<T, F>
where
    T: Clone,
    F: Fn(&T) -> bool + Clone,
{
    predicate: F,
    required_fields: HashSet<String>,
    _phantom: std::marker::PhantomData<T>, // Add phantom type to use T
}

// Custom implementation of Debug for PredicateFilter
// This avoids requiring the F type parameter to implement Debug
impl<T, F> std::fmt::Debug for PredicateFilter<T, F>
where
    T: Clone,
    F: Fn(&T) -> bool + Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PredicateFilter")
            .field("required_fields", &self.required_fields)
            .finish_non_exhaustive() // Skip the predicate field which doesn't implement Debug
    }
}

impl<T, F> PredicateFilter<T, F>
where
    T: Clone,
    F: Fn(&T) -> bool + Clone,
{
    fn new<I: IntoIterator<Item = String>>(predicate: F, required_fields: I) -> Self {
        Self {
            predicate,
            required_fields: required_fields.into_iter().collect(),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T, F> Filter<T> for PredicateFilter<T, F>
where
    T: Clone,
    F: Fn(&T) -> bool + Clone,
{
    fn apply(&self, input: &T) -> Result<T> {
        if (self.predicate)(input) {
            Ok(input.clone())
        } else {
            Err(ParquetReaderError::FilterExcluded {
                message: "Predicate filter excluded the entity".to_string(),
            }
            .into())
        }
    }

    fn required_resources(&self) -> HashSet<String> {
        self.required_fields.clone()
    }
}

// Domain-specific filters for individuals

// Age filter
#[derive(Debug, Clone)]
struct AgeFilter {
    max_age: u32,
}

impl Filter<Individual> for AgeFilter {
    fn apply(&self, input: &Individual) -> Result<Individual> {
        // Calculate age using the reference date of 2023-01-01
        let reference_date = chrono::NaiveDate::from_ymd_opt(2023, 1, 1).unwrap();

        if let Some(calculated_age) = input.age_at(&reference_date) {
            if calculated_age <= self.max_age as i32 {
                Ok(input.clone())
            } else {
                Err(ParquetReaderError::FilterExcluded {
                    message: format!(
                        "Age {} is greater than maximum {}",
                        calculated_age, self.max_age
                    ),
                }
                .into())
            }
        } else {
            Err(ParquetReaderError::FilterExcluded {
                message: "Individual has no calculable age (missing birth_date or was not alive at reference date)".to_string(),
            }
            .into())
        }
    }

    fn required_resources(&self) -> HashSet<String> {
        let mut resources = HashSet::new();
        resources.insert("birth_date".to_string());
        resources
    }
}

// Gender filter
#[derive(Debug, Clone)]
struct GenderFilter {
    gender: Gender,
}

impl Filter<Individual> for GenderFilter {
    fn apply(&self, input: &Individual) -> Result<Individual> {
        if input.gender == self.gender {
            Ok(input.clone())
        } else {
            Err(ParquetReaderError::FilterExcluded {
                message: format!(
                    "Gender {:?} doesn't match required {:?}",
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

// Domain-specific filters for families

// Family size filter
#[derive(Debug, Clone)]
struct FamilySizeFilter {
    min_children: Option<usize>,
    max_children: Option<usize>,
}

impl Filter<Family> for FamilySizeFilter {
    fn apply(&self, input: &Family) -> Result<Family> {
        let size = input.children.len();

        // Check minimum size constraint if specified
        if let Some(min) = self.min_children {
            if size < min {
                return Err(ParquetReaderError::FilterExcluded {
                    message: format!("Family size {size} is less than minimum {min}"),
                }
                .into());
            }
        }

        // Check maximum size constraint if specified
        if let Some(max) = self.max_children {
            if size > max {
                return Err(ParquetReaderError::FilterExcluded {
                    message: format!("Family size {size} is greater than maximum {max}"),
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

// Rural area filter for families
#[derive(Debug, Clone)]
struct FamilyRuralFilter {
    is_rural: bool,
}

impl Filter<Family> for FamilyRuralFilter {
    fn apply(&self, input: &Family) -> Result<Family> {
        if input.is_rural == self.is_rural {
            Ok(input.clone())
        } else {
            Err(ParquetReaderError::FilterExcluded {
                message: format!(
                    "Family rural status {} doesn't match required {}",
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
