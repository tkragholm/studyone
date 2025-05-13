//! Trait definitions for domain models
//!
//! This module defines the core traits that all domain models implement,
//! providing common functionality and interfaces for working with models.

use crate::error::Result;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use std::hash::Hash;

/// A trait that all domain models must implement.
///
/// The `EntityModel` trait provides common functionality for all models
/// in the system, including identifier access and conversion methods.
pub trait EntityModel: Clone + Send + Sync + std::fmt::Debug {
    /// The type of identifier used for this model
    type Id: Clone + Eq + Hash + Send + Sync + std::fmt::Debug;

    /// Get the unique identifier for this model
    fn id(&self) -> &Self::Id;

    /// Create a unique key string representation of the identifier
    fn key(&self) -> String;
}

/// A trait for models that can be converted to and from Arrow `RecordBatch`.
///
/// `ArrowSchema` provides methods for working with Arrow data structures,
/// supporting serialization and deserialization of models to/from `RecordBatch`.
pub trait ArrowSchema: Sized {
    /// Get the Arrow schema for this model
    fn schema() -> Schema;

    /// Convert a `RecordBatch` to a vector of this model
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>>;

    /// Convert a vector of this model to a `RecordBatch`
    fn to_record_batch(models: &[Self]) -> Result<RecordBatch>;
    
    /// Get the schema as Arc<Schema>
    fn schema_ref() -> std::sync::Arc<Schema> {
        std::sync::Arc::new(Self::schema())
    }
}

/// A trait for entities that have temporal validity.
///
/// `TemporalValidity` provides methods for checking if an entity
/// was valid at a specific point in time.
pub trait TemporalValidity {
    /// Check if this entity was valid at a specific date
    fn was_valid_at(&self, date: &NaiveDate) -> bool;

    /// Get the start date of validity
    fn valid_from(&self) -> NaiveDate;

    /// Get the end date of validity (if any)
    fn valid_to(&self) -> Option<NaiveDate>;

    /// Create a snapshot of this entity at a specific point in time
    /// Returns None if the entity was not valid at the given date
    fn snapshot_at(&self, date: &NaiveDate) -> Option<Self>
    where
        Self: Sized;
}

/// A trait for entities that have health status information.
///
/// `HealthStatus` provides methods for checking the health condition
/// of an entity at specific points in time.
pub trait HealthStatus {
    /// Check if the entity was alive at a specific date
    fn was_alive_at(&self, date: &NaiveDate) -> bool;

    /// Check if the entity was resident in Denmark at a specific date
    fn was_resident_at(&self, date: &NaiveDate) -> bool;

    /// Calculate age at a specific reference date
    fn age_at(&self, reference_date: &NaiveDate) -> Option<i32>;
}

/// A trait for filtering collections based on common criteria.
///
/// `Filterable` provides standard filtering methods for collections
/// of entities.
pub trait Filterable<T, C>
where
    T: EntityModel,
{
    /// Filter by a specific time point
    fn filter_by_date(&self, date: &NaiveDate) -> C;

    /// Filter by an attribute value
    fn filter_by_attribute<V: PartialEq>(&self, attr_name: &str, value: V) -> C;

    /// Apply multiple filters in sequence
    fn apply_filters<F>(&self, filters: Vec<F>) -> C
    where
        F: Fn(&T) -> bool;
}
