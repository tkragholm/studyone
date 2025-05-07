//! Generic filtering framework
//!
//! This module provides a generic trait-based approach to filtering various data types.
//! It serves as a foundation for both Arrow/Parquet filtering and domain entity filtering.

use std::collections::HashSet;
use std::fmt::Debug;

use crate::error::{ParquetReaderError, Result};

/// A generic filter trait that can be applied to any data type
pub trait Filter<T>: Debug {
    /// Apply the filter to an input value
    ///
    /// # Arguments
    /// * `input` - The value to filter
    ///
    /// # Returns
    /// A filtered result
    ///
    /// # Errors
    /// Returns an error if filtering fails
    fn apply(&self, input: &T) -> Result<T>;
    
    /// Returns the set of resources required by this filter
    ///
    /// This might be column names for a record batch filter,
    /// or field names for a domain entity filter.
    fn required_resources(&self) -> HashSet<String>;
}

/// A filter that always includes all elements
#[derive(Debug, Clone, Default)]
pub struct IncludeAllFilter;

/// Implementation for IncludeAllFilter that accepts any type
impl<T: Clone + Debug> Filter<T> for IncludeAllFilter {
    fn apply(&self, input: &T) -> Result<T> {
        // Always include the input by cloning it
        Ok(input.clone())
    }

    fn required_resources(&self) -> HashSet<String> {
        // Include all filter doesn't require any specific resources
        HashSet::new()
    }
}

/// A filter that excludes all elements
#[derive(Debug, Clone, Default)]
pub struct ExcludeAllFilter;

/// Implementation for ExcludeAllFilter that rejects any type
impl<T: Debug> Filter<T> for ExcludeAllFilter {
    fn apply(&self, _input: &T) -> Result<T> {
        // Always exclude with standard error
        Err(ParquetReaderError::FilterExcluded {
            message: "Excluded by ExcludeAllFilter".to_string(),
        }
        .into())
    }

    fn required_resources(&self) -> HashSet<String> {
        // Exclude all filter doesn't require any specific resources
        HashSet::new()
    }
}

/// A filter that combines multiple filters with a logical AND
#[derive(Clone)]
pub struct AndFilter<T, F>
where
    F: Filter<T> + Send + Sync + Clone,
    T: Debug,
{
    filters: Vec<F>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T, F> Debug for AndFilter<T, F>
where
    F: Filter<T> + Send + Sync + Debug + Clone,
    T: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AndFilter")
            .field("filters", &self.filters)
            .finish()
    }
}

impl<T, F> AndFilter<T, F>
where
    F: Filter<T> + Send + Sync + Clone,
    T: Debug,
{
    /// Create a new AND filter
    #[must_use]
    pub fn new(filters: Vec<F>) -> Self {
        Self {
            filters,
            _phantom: std::marker::PhantomData,
        }
    }
}

/// Implementation of Filter for AndFilter
impl<T, F> Filter<T> for AndFilter<T, F>
where
    F: Filter<T> + Send + Sync + Clone,
    T: Clone + Debug + Send + Sync,
{
    fn apply(&self, input: &T) -> Result<T> {
        // If no filters, include all
        if self.filters.is_empty() {
            return Ok(input.clone());
        }

        // All filters must succeed
        let mut result = input.clone();

        // Apply each filter in sequence
        for filter in &self.filters {
            // Apply the filter to the current result
            result = filter.apply(&result)?;
        }

        // If we made it here, all filters passed
        Ok(result)
    }

    fn required_resources(&self) -> HashSet<String> {
        // Union all required resources from component filters
        let mut resources = HashSet::new();
        for filter in &self.filters {
            resources.extend(filter.required_resources().into_iter());
        }
        resources
    }
}

/// A filter that combines multiple filters with a logical OR
#[derive(Clone)]
pub struct OrFilter<T, F>
where
    F: Filter<T> + Send + Sync + Clone,
    T: Debug,
{
    filters: Vec<F>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T, F> Debug for OrFilter<T, F>
where
    F: Filter<T> + Send + Sync + Debug + Clone,
    T: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OrFilter")
            .field("filters", &self.filters)
            .finish()
    }
}

impl<T, F> OrFilter<T, F>
where
    F: Filter<T> + Send + Sync + Clone,
    T: Debug,
{
    /// Create a new OR filter
    #[must_use]
    pub fn new(filters: Vec<F>) -> Self {
        Self {
            filters,
            _phantom: std::marker::PhantomData,
        }
    }
}

/// Implementation of Filter for OrFilter
impl<T, F> Filter<T> for OrFilter<T, F>
where
    F: Filter<T> + Send + Sync + Clone,
    T: Clone + Debug + Send + Sync,
{
    fn apply(&self, input: &T) -> Result<T> {
        // If no filters, exclude all
        if self.filters.is_empty() {
            return Err(ParquetReaderError::FilterExcluded {
                message: "Empty OrFilter excludes all inputs".to_string(),
            }
            .into());
        }

        // Try each filter until one passes
        let mut last_error = None;
        
        for filter in &self.filters {
            match filter.apply(input) {
                Ok(result) => return Ok(result), // This filter passed
                Err(e) => last_error = Some(e),  // Save the error and try the next filter
            }
        }

        // If we're here, no filter passed - return the last error
        Err(last_error.unwrap_or_else(|| 
            ParquetReaderError::FilterExcluded {
                message: "All filters in OrFilter rejected the input".to_string(),
            }.into()
        ))
    }

    fn required_resources(&self) -> HashSet<String> {
        // Union all required resources from component filters
        let mut resources = HashSet::new();
        for filter in &self.filters {
            resources.extend(filter.required_resources().into_iter());
        }
        resources
    }
}

/// A filter that negates another filter
#[derive(Clone)]
pub struct NotFilter<T, F>
where
    F: Filter<T> + Send + Sync + Clone,
    T: Debug,
{
    filter: F,
    _phantom: std::marker::PhantomData<T>,
}

impl<T, F> Debug for NotFilter<T, F>
where
    F: Filter<T> + Send + Sync + Debug + Clone,
    T: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NotFilter")
            .field("filter", &self.filter)
            .finish()
    }
}

impl<T, F> NotFilter<T, F>
where
    F: Filter<T> + Send + Sync + Clone,
    T: Debug,
{
    /// Create a new NOT filter
    #[must_use]
    pub fn new(filter: F) -> Self {
        Self {
            filter,
            _phantom: std::marker::PhantomData,
        }
    }
}

/// Implementation of Filter for NotFilter
impl<T, F> Filter<T> for NotFilter<T, F>
where
    F: Filter<T> + Send + Sync + Clone,
    T: Clone + Debug + Send + Sync,
{
    fn apply(&self, input: &T) -> Result<T> {
        // Try to apply the inner filter
        match self.filter.apply(input) {
            // If the inner filter passed, we fail
            Ok(_) => Err(ParquetReaderError::FilterExcluded {
                message: "NotFilter excluded item that passed inner filter".to_string(),
            }.into()),
            
            // If the inner filter failed, we pass
            Err(_) => Ok(input.clone()),
        }
    }

    fn required_resources(&self) -> HashSet<String> {
        // Require the same resources as the inner filter
        self.filter.required_resources()
    }
}

/// A filter adapter that transforms one filter type to another
pub trait FilterAdapter<T, U> {
    /// Create a filter for type U from a filter for type T
    fn adapt<F: Filter<T>>(&self, filter: F) -> Box<dyn Filter<U> + Send + Sync>;
}

/// A trait for objects that can create expressions for filtering
pub trait FilterExpressionBuilder<T> {
    /// Type of the expression created
    type Expression;
    
    /// Create an equality expression
    fn eq(&self, field: &str, value: T) -> Self::Expression;
    
    /// Create a less than expression
    fn lt(&self, field: &str, value: T) -> Self::Expression;
    
    /// Create a greater than expression
    fn gt(&self, field: &str, value: T) -> Self::Expression;
    
    /// Create a less than or equal expression
    fn lte(&self, field: &str, value: T) -> Self::Expression;
    
    /// Create a greater than or equal expression
    fn gte(&self, field: &str, value: T) -> Self::Expression;
    
    /// Create an in-set expression
    fn contains(&self, field: &str, values: &[T]) -> Self::Expression;
    
    /// Create a logical AND expression
    fn and(&self, left: Self::Expression, right: Self::Expression) -> Self::Expression;
    
    /// Create a logical OR expression
    fn or(&self, left: Self::Expression, right: Self::Expression) -> Self::Expression;
    
    /// Create a logical NOT expression
    fn not(&self, expr: Self::Expression) -> Self::Expression;
}

/// A generic builder for creating filter instances
///
/// This provides a fluent interface for combining filters in a type-safe way.
#[derive(Debug, Clone)]
pub struct FilterBuilder<T, F>
where
    F: Filter<T> + Send + Sync + Clone,
    T: Clone + Debug + Send + Sync,
{
    filters: Vec<F>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T, F> FilterBuilder<T, F>
where
    F: Filter<T> + Send + Sync + Clone,
    T: Clone + Debug + Send + Sync,
{
    /// Create a new filter builder
    pub fn new() -> Self {
        Self {
            filters: Vec::new(),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Add a filter to the builder
    pub fn add_filter(mut self, filter: F) -> Self {
        self.filters.push(filter);
        self
    }

    /// Combine all filters with AND
    pub fn build_and(self) -> AndFilter<T, F> {
        AndFilter::new(self.filters)
    }

    /// Combine all filters with OR
    pub fn build_or(self) -> OrFilter<T, F> {
        OrFilter::new(self.filters)
    }

    /// Create a filter that is the logical NOT of all the filters combined with AND
    pub fn build_not_and(self) -> NotFilter<T, AndFilter<T, F>> {
        NotFilter::new(self.build_and())
    }

    /// Create a filter that is the logical NOT of all the filters combined with OR
    pub fn build_not_or(self) -> NotFilter<T, OrFilter<T, F>> {
        NotFilter::new(self.build_or())
    }
}

impl<T, F> Default for FilterBuilder<T, F>
where
    F: Filter<T> + Send + Sync + Clone,
    T: Clone + Debug + Send + Sync,
{
    fn default() -> Self {
        Self::new()
    }
}

//// A type-erased filter that can store any filter implementation
pub struct BoxedFilter<T: Clone + Debug + Send + Sync + 'static> {
    // Box<dyn Trait> isn't Clone, so we use an Arc to allow cloning
    inner: std::sync::Arc<dyn Filter<T> + Send + Sync>,
}

impl<T: Clone + Debug + Send + Sync + 'static> BoxedFilter<T> {
    /// Create a new boxed filter
    pub fn new<F>(filter: F) -> Self
    where
        F: Filter<T> + Send + Sync + 'static,
    {
        Self {
            inner: std::sync::Arc::new(filter),
        }
    }
}

impl<T: Clone + Debug + Send + Sync + 'static> Clone for BoxedFilter<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T: Clone + Debug + Send + Sync + 'static> Debug for BoxedFilter<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoxedFilter")
            .field("inner", &self.inner)
            .finish()
    }
}

impl<T: Clone + Debug + Send + Sync + 'static> Filter<T> for BoxedFilter<T> {
    fn apply(&self, input: &T) -> Result<T> {
        self.inner.apply(input)
    }

    fn required_resources(&self) -> HashSet<String> {
        self.inner.required_resources()
    }
}

/// Custom filter for AND operations on two filters
#[derive(Debug, Clone)]
struct AndCombinator<T, A, B> 
where 
    T: Clone + Debug + Send + Sync + 'static,
    A: Filter<T> + Send + Sync + 'static,
    B: Filter<T> + Send + Sync + 'static,
{
    first: A,
    second: B,
    _phantom: std::marker::PhantomData<T>,
}

impl<T, A, B> Filter<T> for AndCombinator<T, A, B>
where
    T: Clone + Debug + Send + Sync + 'static,
    A: Filter<T> + Send + Sync + 'static,
    B: Filter<T> + Send + Sync + 'static,
{
    fn apply(&self, input: &T) -> Result<T> {
        // Apply first filter
        let result = self.first.apply(input)?;
        
        // Apply second filter to the result of the first
        self.second.apply(&result)
    }

    fn required_resources(&self) -> HashSet<String> {
        // Combine required resources from both filters
        let mut resources = self.first.required_resources();
        resources.extend(self.second.required_resources().into_iter());
        resources
    }
}

/// Custom filter for OR operations on two filters
#[derive(Debug, Clone)]
struct OrCombinator<T, A, B> 
where 
    T: Clone + Debug + Send + Sync + 'static,
    A: Filter<T> + Send + Sync + 'static,
    B: Filter<T> + Send + Sync + 'static,
{
    first: A,
    second: B,
    _phantom: std::marker::PhantomData<T>,
}

impl<T, A, B> Filter<T> for OrCombinator<T, A, B>
where
    T: Clone + Debug + Send + Sync + 'static,
    A: Filter<T> + Send + Sync + 'static,
    B: Filter<T> + Send + Sync + 'static,
{
    fn apply(&self, input: &T) -> Result<T> {
        // Try the first filter
        match self.first.apply(input) {
            Ok(result) => Ok(result), // First filter passed
            Err(_) => {
                // Try the second filter
                match self.second.apply(input) {
                    Ok(result) => Ok(result), // Second filter passed
                    Err(_) => {
                        // Both filters failed
                        Err(ParquetReaderError::FilterExcluded {
                            message: "Both filters in OR operation rejected the input".to_string(),
                        }.into())
                    }
                }
            }
        }
    }

    fn required_resources(&self) -> HashSet<String> {
        // Combine required resources from both filters
        let mut resources = self.first.required_resources();
        resources.extend(self.second.required_resources().into_iter());
        resources
    }
}

/// Custom filter for NOT operations on a filter
#[derive(Debug, Clone)]
struct NotCombinator<T, F> 
where 
    T: Clone + Debug + Send + Sync + 'static,
    F: Filter<T> + Send + Sync + 'static,
{
    inner: F,
    _phantom: std::marker::PhantomData<T>,
}

impl<T, F> Filter<T> for NotCombinator<T, F>
where
    T: Clone + Debug + Send + Sync + 'static,
    F: Filter<T> + Send + Sync + 'static,
{
    fn apply(&self, input: &T) -> Result<T> {
        // Try to apply the inner filter
        match self.inner.apply(input) {
            // If the inner filter passed, we fail
            Ok(_) => Err(ParquetReaderError::FilterExcluded {
                message: "NOT filter excluded item that passed inner filter".to_string(),
            }.into()),
            
            // If the inner filter failed, we pass
            Err(_) => Ok(input.clone()),
        }
    }

    fn required_resources(&self) -> HashSet<String> {
        // Require the same resources as the inner filter
        self.inner.required_resources()
    }
}

/// Extension trait that adds utility methods to any Filter
pub trait FilterExt<T: Clone + Debug + Send + Sync + 'static>: Filter<T> + Sized {
    /// Create a new filter that is the AND of this filter and another
    fn and<F: Filter<T> + Send + Sync + 'static>(self, other: F) -> BoxedFilter<T>
    where
        Self: 'static + Send + Sync,
    {
        let combinator = AndCombinator {
            first: self,
            second: other,
            _phantom: std::marker::PhantomData,
        };
        
        BoxedFilter::new(combinator)
    }

    /// Create a new filter that is the OR of this filter and another
    fn or<F: Filter<T> + Send + Sync + 'static>(self, other: F) -> BoxedFilter<T>
    where
        Self: 'static + Send + Sync,
    {
        let combinator = OrCombinator {
            first: self,
            second: other,
            _phantom: std::marker::PhantomData,
        };
        
        BoxedFilter::new(combinator)
    }

    /// Create a new filter that is the logical NOT of this filter
    fn not(self) -> BoxedFilter<T>
    where
        Self: 'static + Send + Sync,
    {
        let combinator = NotCombinator {
            inner: self,
            _phantom: std::marker::PhantomData,
        };
        
        BoxedFilter::new(combinator)
    }
}

// Implement FilterExt for all types that implement Filter
impl<T: Clone + Debug + Send + Sync + 'static, F: Filter<T>> FilterExt<T> for F {}