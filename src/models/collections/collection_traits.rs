//! Collection trait definitions for domain models
//!
//! This module defines the traits for collections of domain models,
//! providing common functionality for storing and querying model collections.

use crate::models::core::traits::EntityModel;
use std::hash::Hash;
use std::sync::Arc;

/// A trait for collections of models that can be queried and modified.
///
/// `ModelCollection` provides methods for storing, retrieving, and
/// filtering collections of models.
pub trait ModelCollection<T: EntityModel> {
    /// Add a model to the collection
    fn add(&mut self, model: T);

    /// Get a model by its identifier
    fn get(&self, id: &T::Id) -> Option<Arc<T>>;

    /// Get all models in the collection
    fn all(&self) -> Vec<Arc<T>>;

    /// Filter models by a predicate function
    fn filter<F>(&self, predicate: F) -> Vec<Arc<T>>
    where
        F: Fn(&T) -> bool;

    /// Count the total number of models in the collection
    fn count(&self) -> usize;
}

/// A trait for an indexed collection that can be queried efficiently.
///
/// `IndexedCollection` extends the basic `ModelCollection` with methods
/// for retrieving models by various indexes beyond just their primary ID.
pub trait IndexedCollection<T: EntityModel>: ModelCollection<T> {
    /// Get models by a secondary index field
    fn get_by_index<K: Eq + Hash + Clone>(&self, index_name: &str, key: K) -> Vec<Arc<T>>;

    /// Check if an index exists
    fn has_index(&self, index_name: &str) -> bool;

    /// Create a new index on a field
    fn create_index<F, K>(&mut self, index_name: &str, key_fn: F)
    where
        F: Fn(&T) -> K,
        K: Eq + Hash + Clone;

    /// Drop an existing index
    fn drop_index(&mut self, index_name: &str);
}

/// Helper trait for batch operations on collections
pub trait BatchOperations<T: EntityModel>: ModelCollection<T> {
    /// Add multiple models at once
    fn add_batch(&mut self, models: Vec<T>);

    /// Remove models by their identifiers
    fn remove_batch(&mut self, ids: &[T::Id]) -> Vec<Arc<T>>;

    /// Update multiple models at once
    fn update_batch<F>(&mut self, ids: &[T::Id], update_fn: F)
    where
        F: Fn(&mut T);
}
