//! Standardized collection traits
//!
//! This module defines traits for collections of domain models, providing
//! a unified interface for working with collections across the codebase.
//! It standardizes common operations like adding, getting, and filtering items.

use crate::error::Result;
use crate::models::traits::{EntityModel, TemporalValidity};
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

/// Core trait for model collections
///
/// This trait provides the fundamental operations that all model collections
/// should support, including adding, getting, and listing items.
pub trait ModelCollection<T: EntityModel>: Send + Sync + std::fmt::Debug {
    /// Add a model to the collection
    fn add(&mut self, model: T);
    
    /// Get a model by its identifier
    fn get(&self, id: &T::Id) -> Option<Arc<T>>;
    
    /// Get all models in the collection
    fn all(&self) -> Vec<Arc<T>>;
    
    /// Count the total number of models in the collection
    fn count(&self) -> usize;
    
    /// Filter models by a predicate function
    fn filter<F>(&self, predicate: F) -> Vec<Arc<T>>
    where
        F: Fn(&T) -> bool;
        
    /// Check if the collection contains a model with the given ID
    fn contains(&self, id: &T::Id) -> bool {
        self.get(id).is_some()
    }
    
    /// Add multiple models to the collection
    fn add_all(&mut self, models: Vec<T>) {
        for model in models {
            self.add(model);
        }
    }
}

/// Trait for collections with temporal filtering capabilities
///
/// This trait extends the basic collection functionality with methods
/// for working with temporal data, including filtering by date ranges
/// and creating temporal snapshots.
pub trait TemporalCollection<T>: ModelCollection<T> 
where 
    T: EntityModel + TemporalValidity
{
    /// Get all models valid at a specific date
    fn valid_at(&self, date: &NaiveDate) -> Vec<Arc<T>> {
        self.filter(|model| model.was_valid_at(date))
    }
    
    /// Get all models valid during a date range
    fn valid_during(&self, start_date: &NaiveDate, end_date: &NaiveDate) -> Vec<Arc<T>> {
        self.filter(|model| {
            // Valid if the model's validity period overlaps with the given range
            let valid_from = model.valid_from();
            let valid_to = model.valid_to().unwrap_or(chrono::NaiveDate::MAX);
            
            valid_from <= *end_date && valid_to >= *start_date
        })
    }
    
    /// Create snapshots of all valid models at a specific date
    fn snapshots_at(&self, date: &NaiveDate) -> Vec<T> {
        self.valid_at(date)
            .iter()
            .filter_map(|model| model.snapshot_at(date))
            .collect()
    }
}

/// Trait for collections that support batch operations
///
/// This trait provides methods for efficient batch processing of models,
/// including loading from `RecordBatch` and performing bulk updates.
pub trait BatchCollection<T: EntityModel>: ModelCollection<T> {
    /// Load models from a `RecordBatch`
    fn load_from_batch(&mut self, batch: &RecordBatch) -> Result<()>;
    
    /// Update models with data from a `RecordBatch`
    fn update_from_batch(&mut self, batch: &RecordBatch) -> Result<()>;
    
    /// Export models to a `RecordBatch`
    fn export_to_batch(&self) -> Result<RecordBatch>;
}

/// Trait for creating lookups from collections
///
/// This trait provides methods for creating efficient lookup maps
/// from collections of models, using various key functions.
pub trait LookupCollection<T: EntityModel>: ModelCollection<T> {
    /// Create a lookup map using a key function
    fn create_lookup<K, F>(&self, key_fn: F) -> HashMap<K, Arc<T>>
    where
        K: Eq + Hash,
        F: Fn(&T) -> K,
    {
        let mut lookup = HashMap::with_capacity(self.count());
        for model in self.all() {
            let key = key_fn(&model);
            lookup.insert(key, model);
        }
        lookup
    }
    
    /// Create a lookup map using multiple values per key
    fn create_multi_lookup<K, F>(&self, key_fn: F) -> HashMap<K, Vec<Arc<T>>>
    where
        K: Eq + Hash,
        F: Fn(&T) -> K,
    {
        let mut lookup = HashMap::new();
        for model in self.all() {
            let key = key_fn(&model);
            lookup.entry(key).or_insert_with(Vec::new).push(model);
        }
        lookup
    }
}

/// Trait for collections that maintain relationships with other entity types
///
/// This trait provides methods for working with collections that contain
/// relationships between different entity types.
pub trait RelatedCollection<T: EntityModel, R: EntityModel>: ModelCollection<T> {
    /// Get related entities for a model
    fn get_related(&self, model: &T) -> Vec<Arc<R>>;
    
    /// Get related entities for a model ID
    fn get_related_by_id(&self, id: &T::Id) -> Vec<Arc<R>> {
        match self.get(id) {
            Some(model) => self.get_related(&model),
            None => Vec::new(),
        }
    }
    
    /// Find all models related to a specific entity
    fn find_by_related(&self, related: &R) -> Vec<Arc<T>>;
    
    /// Find all models related to a specific entity ID
    fn find_by_related_id(&self, related_id: &R::Id) -> Vec<Arc<T>>;
}

/// Trait for collections that support caching for performance
///
/// This trait provides methods for managing cached data in collections
/// to improve performance for frequently accessed queries.
pub trait CacheableCollection<T: EntityModel>: ModelCollection<T> {
    /// Clear all cached data in the collection
    fn clear_cache(&mut self);
    
    /// Update the cache with new data
    fn update_cache(&mut self);
    
    /// Check if the cache is valid
    fn is_cache_valid(&self) -> bool;
}