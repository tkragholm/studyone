//! Standardized model collections
//!
//! This module provides generic collection implementations that follow
//! the standardized collection interfaces defined in the common traits.
//! These collections can be used directly or extended for specific model types.
//!
//! The module includes both generic collections that work with any model type
//! and specialized collections that provide domain-specific functionality.

// Specialized collection modules
// pub mod diagnosis;
// pub mod family;
pub mod individual;

// Re-export specialized collections for convenience
// pub use diagnosis::DiagnosisCollection;
// pub use family::FamilyCollection;
pub use individual::IndividualCollection;

use crate::common::traits::{
    BatchCollection, CacheableCollection, LookupCollection, ModelCollection, TemporalCollection,
};
use crate::error::Result;
use crate::models::{ArrowSchema, EntityModel, TemporalValidity};
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;

/// Generic model collection implementation
///
/// This collection provides a standard implementation of the `ModelCollection`
/// trait for any model type that implements `EntityModel`. It stores models
/// in a `HashMap` indexed by their ID for efficient access.
#[derive(Debug)]
pub struct GenericCollection<T: EntityModel> {
    /// Models indexed by ID
    items: HashMap<T::Id, Arc<T>>,
}

impl<T: EntityModel> GenericCollection<T> {
    /// Create a new empty collection
    #[must_use]
    pub fn new() -> Self {
        Self {
            items: HashMap::new(),
        }
    }

    /// Create a collection from a vector of models
    #[must_use]
    pub fn from_models(models: Vec<T>) -> Self {
        let mut collection = Self::new();
        for model in models {
            collection.add(model);
        }
        collection
    }

    /// Get all model IDs in the collection
    #[must_use]
    pub fn ids(&self) -> Vec<T::Id>
    where
        T::Id: Clone,
    {
        self.items.keys().cloned().collect()
    }

    /// Remove a model from the collection
    pub fn remove(&mut self, id: &T::Id) -> Option<Arc<T>> {
        self.items.remove(id)
    }

    /// Clear all models from the collection
    pub fn clear(&mut self) {
        self.items.clear();
    }

    /// Get a map of all items by their ID
    #[must_use]
    pub const fn as_map(&self) -> &HashMap<T::Id, Arc<T>> {
        &self.items
    }
}

impl<T: EntityModel> Default for GenericCollection<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: EntityModel> ModelCollection<T> for GenericCollection<T> {
    fn add(&mut self, model: T) {
        let id = model.id().clone();
        self.items.insert(id, Arc::new(model));
    }

    fn get(&self, id: &T::Id) -> Option<Arc<T>> {
        self.items.get(id).cloned()
    }

    fn all(&self) -> Vec<Arc<T>> {
        self.items.values().cloned().collect()
    }

    fn count(&self) -> usize {
        self.items.len()
    }

    fn filter<F>(&self, predicate: F) -> Vec<Arc<T>>
    where
        F: Fn(&T) -> bool,
    {
        self.items
            .values()
            .filter(|model| predicate(model))
            .cloned()
            .collect()
    }
}

impl<T> TemporalCollection<T> for GenericCollection<T>
where
    T: EntityModel + TemporalValidity,
{
    // All methods are inherited from the default implementations
}

impl<T> LookupCollection<T> for GenericCollection<T>
where
    T: EntityModel,
{
    // All methods are inherited from the default implementations
}

impl<T> BatchCollection<T> for GenericCollection<T>
where
    T: EntityModel + ArrowSchema,
{
    fn load_from_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let models = T::from_record_batch(batch)?;
        for model in models {
            self.add(model);
        }
        Ok(())
    }

    fn update_from_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let models = T::from_record_batch(batch)?;
        for model in models {
            let id = model.id().clone();
            self.items.insert(id, Arc::new(model));
        }
        Ok(())
    }

    fn export_to_batch(&self) -> Result<RecordBatch> {
        let models: Vec<T> = self.all().iter().map(|arc| (**arc).clone()).collect();

        T::to_record_batch(&models)
    }
}

/// A temporal model collection that maintains cached snapshots
///
/// This collection extends `GenericCollection` with caching capabilities for
/// temporal snapshots, improving performance for frequently accessed dates.
#[derive(Debug)]
pub struct TemporalCollectionWithCache<T>
where
    T: EntityModel + TemporalValidity + Clone,
{
    /// The base collection
    inner: GenericCollection<T>,
    /// Cached snapshots by date
    snapshot_cache: HashMap<NaiveDate, Vec<T>>,
    /// Dates that have cached snapshots
    cached_dates: HashSet<NaiveDate>,
    /// Flag indicating whether the cache is valid
    cache_valid: bool,
}

impl<T> TemporalCollectionWithCache<T>
where
    T: EntityModel + TemporalValidity + Clone,
{
    /// Create a new empty collection with caching
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: GenericCollection::new(),
            snapshot_cache: HashMap::new(),
            cached_dates: HashSet::new(),
            cache_valid: true,
        }
    }

    /// Create a collection from a vector of models
    #[must_use]
    pub fn from_models(models: Vec<T>) -> Self {
        Self {
            inner: GenericCollection::from_models(models),
            snapshot_cache: HashMap::new(),
            cached_dates: HashSet::new(),
            cache_valid: true,
        }
    }

    /// Get cached snapshots for a specific date, calculating if needed
    #[must_use]
    pub fn get_snapshots(&mut self, date: &NaiveDate) -> Vec<T> {
        // If cache is invalid or this date isn't cached, calculate snapshots
        if !self.cache_valid || !self.cached_dates.contains(date) {
            let snapshots = self.snapshots_at(date);
            self.snapshot_cache.insert(*date, snapshots.clone());
            self.cached_dates.insert(*date);
            snapshots
        } else {
            // Return from cache
            self.snapshot_cache.get(date).cloned().unwrap_or_default()
        }
    }
}

impl<T> Default for TemporalCollectionWithCache<T>
where
    T: EntityModel + TemporalValidity + Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T> ModelCollection<T> for TemporalCollectionWithCache<T>
where
    T: EntityModel + TemporalValidity + Clone,
{
    fn add(&mut self, model: T) {
        self.inner.add(model);
        self.cache_valid = false;
    }

    fn get(&self, id: &T::Id) -> Option<Arc<T>> {
        self.inner.get(id)
    }

    fn all(&self) -> Vec<Arc<T>> {
        self.inner.all()
    }

    fn count(&self) -> usize {
        self.inner.count()
    }

    fn filter<F>(&self, predicate: F) -> Vec<Arc<T>>
    where
        F: Fn(&T) -> bool,
    {
        self.inner.filter(predicate)
    }
}

impl<T> TemporalCollection<T> for TemporalCollectionWithCache<T>
where
    T: EntityModel + TemporalValidity + Clone,
{
    // Use the default implementations from the trait
}

impl<T> LookupCollection<T> for TemporalCollectionWithCache<T>
where
    T: EntityModel + TemporalValidity + Clone,
{
    // Use the default implementations from the trait
}

impl<T> CacheableCollection<T> for TemporalCollectionWithCache<T>
where
    T: EntityModel + TemporalValidity + Clone,
{
    fn clear_cache(&mut self) {
        self.snapshot_cache.clear();
        self.cached_dates.clear();
        self.cache_valid = true;
    }

    fn update_cache(&mut self) {
        let dates = self.cached_dates.clone();

        // Recalculate snapshots for all cached dates
        for date in dates {
            let snapshots = self.snapshots_at(&date);
            self.snapshot_cache.insert(date, snapshots);
        }

        self.cache_valid = true;
    }

    fn is_cache_valid(&self) -> bool {
        self.cache_valid
    }
}

/// A multi-model collection that links related entities
///
/// This collection manages relationships between two model types,
/// providing efficient access to related entities.
#[derive(Debug)]
pub struct RelatedModelCollection<P: EntityModel, C: EntityModel> {
    /// The primary model collection
    primary: GenericCollection<P>,
    /// The related model collection
    related: GenericCollection<C>,
    /// Mapping from primary ID to related IDs
    primary_to_related: HashMap<P::Id, Vec<C::Id>>,
    /// Mapping from related ID to primary IDs
    related_to_primary: HashMap<C::Id, Vec<P::Id>>,
    /// Type marker for primary model
    _primary_marker: PhantomData<P>,
    /// Type marker for related model
    _related_marker: PhantomData<C>,
}

impl<P: EntityModel, C: EntityModel> RelatedModelCollection<P, C>
where
    P::Id: Clone + Eq + Hash,
    C::Id: Clone + Eq + Hash,
{
    /// Create a new empty related model collection
    #[must_use]
    pub fn new() -> Self {
        Self {
            primary: GenericCollection::new(),
            related: GenericCollection::new(),
            primary_to_related: HashMap::new(),
            related_to_primary: HashMap::new(),
            _primary_marker: PhantomData,
            _related_marker: PhantomData,
        }
    }

    /// Add a relationship between primary and related models
    pub fn add_relationship(&mut self, primary_id: P::Id, related_id: C::Id) {
        self.primary_to_related
            .entry(primary_id.clone())
            .or_default()
            .push(related_id.clone());

        self.related_to_primary
            .entry(related_id)
            .or_default()
            .push(primary_id);
    }

    /// Remove a relationship between primary and related models
    pub fn remove_relationship(&mut self, primary_id: &P::Id, related_id: &C::Id) {
        // Remove related ID from primary's relationships
        if let Some(related_ids) = self.primary_to_related.get_mut(primary_id) {
            related_ids.retain(|id| id != related_id);
        }

        // Remove primary ID from related's relationships
        if let Some(primary_ids) = self.related_to_primary.get_mut(related_id) {
            primary_ids.retain(|id| id != primary_id);
        }
    }

    /// Get primary models related to a specific related model
    #[must_use]
    pub fn get_primary_for_related(&self, related_id: &C::Id) -> Vec<Arc<P>> {
        let primary_ids = self
            .related_to_primary
            .get(related_id)
            .map_or_else(Vec::new, std::clone::Clone::clone);

        primary_ids
            .iter()
            .filter_map(|id| self.primary.get(id))
            .collect()
    }

    /// Get related models linked to a specific primary model
    #[must_use]
    pub fn get_related_for_primary(&self, primary_id: &P::Id) -> Vec<Arc<C>> {
        let related_ids = self
            .primary_to_related
            .get(primary_id)
            .map_or_else(Vec::new, std::clone::Clone::clone);

        related_ids
            .iter()
            .filter_map(|id| self.related.get(id))
            .collect()
    }

    /// Get the primary model collection
    #[must_use]
    pub const fn primary(&self) -> &GenericCollection<P> {
        &self.primary
    }

    /// Get the related model collection
    #[must_use]
    pub const fn related(&self) -> &GenericCollection<C> {
        &self.related
    }

    /// Get a mutable reference to the primary model collection
    pub const fn primary_mut(&mut self) -> &mut GenericCollection<P> {
        &mut self.primary
    }

    /// Get a mutable reference to the related model collection
    pub const fn related_mut(&mut self) -> &mut GenericCollection<C> {
        &mut self.related
    }
}

impl<P: EntityModel, C: EntityModel> Default for RelatedModelCollection<P, C>
where
    P::Id: Clone + Eq + Hash,
    C::Id: Clone + Eq + Hash,
{
    fn default() -> Self {
        Self::new()
    }
}
