//! Generic filtering framework
//!
//! This module provides a generic trait-based approach to filtering various data types.
//! It serves as a foundation for both Arrow/Parquet filtering and domain entity filtering.

use std::collections::HashSet;
use std::fmt::Debug;

use crate::error::Result;

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

/// A filter that excludes all elements
#[derive(Debug, Clone, Default)]
pub struct ExcludeAllFilter;

/// A filter that combines multiple filters with a logical AND
#[derive(Debug, Clone)]
pub struct AndFilter<T, F>
where
    F: Filter<T> + Send + Sync,
{
    filters: Vec<F>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T, F> AndFilter<T, F>
where
    F: Filter<T> + Send + Sync,
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

/// A filter that combines multiple filters with a logical OR
#[derive(Debug, Clone)]
pub struct OrFilter<T, F>
where
    F: Filter<T> + Send + Sync,
{
    filters: Vec<F>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T, F> OrFilter<T, F>
where
    F: Filter<T> + Send + Sync,
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

/// A filter that negates another filter
#[derive(Debug, Clone)]
pub struct NotFilter<T, F>
where
    F: Filter<T> + Send + Sync,
{
    filter: F,
    _phantom: std::marker::PhantomData<T>,
}

impl<T, F> NotFilter<T, F>
where
    F: Filter<T> + Send + Sync,
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

/// A filter adapter that transforms one filter type to another
pub trait FilterAdapter<T, U> {
    /// Create a filter for type U from a filter for type T
    fn adapt<F: Filter<T>>(&self, filter: F) -> Box<dyn Filter<U>>;
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