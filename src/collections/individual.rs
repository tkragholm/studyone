//! Individual model collection
//!
//! This module provides a specialized collection implementation for Individual models.
//! This is the canonical implementation of `IndividualCollection` in the codebase.

use crate::collections::GenericCollection;
use crate::common::traits::{
    ModelCollection, TemporalCollection, LookupCollection, BatchCollection
};
use crate::models::core::Individual;
use crate::models::core::traits::HealthStatus;
use crate::models::core::types::Gender;
use chrono::NaiveDate;
use std::sync::Arc;
use arrow::record_batch::RecordBatch;
use crate::error::Result;

/// Specialized collection for Individual models
#[derive(Debug, Default)]
pub struct IndividualCollection {
    /// Base generic collection implementation
    inner: GenericCollection<Individual>,
}

impl IndividualCollection {
    /// Create a new empty individual collection
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: GenericCollection::new(),
        }
    }

    /// Create a collection from a vector of individuals
    #[must_use]
    pub fn from_individuals(individuals: Vec<Individual>) -> Self {
        Self {
            inner: GenericCollection::from_models(individuals),
        }
    }

    /// Alternative constructor name for backward compatibility
    #[must_use]
    pub fn with_individuals(individuals: Vec<Individual>) -> Self {
        Self::from_individuals(individuals)
    }

    /// Find individuals that were alive at a specific date
    #[must_use]
    pub fn alive_at(&self, date: &NaiveDate) -> Vec<Arc<Individual>> {
        self.inner.filter(|individual| individual.was_alive_at(date))
    }

    /// Find individuals that were resident at a specific date
    #[must_use]
    pub fn resident_at(&self, date: &NaiveDate) -> Vec<Arc<Individual>> {
        self.inner.filter(|individual| individual.was_resident_at(date))
    }

    /// Find individuals within a specific age range at a date
    #[must_use]
    pub fn age_between(
        &self,
        date: &NaiveDate,
        min_age: i32,
        max_age: i32,
    ) -> Vec<Arc<Individual>> {
        self.inner.filter(|individual| {
            if let Some(age) = individual.age_at(date) {
                (min_age..=max_age).contains(&age)
            } else {
                false
            }
        })
    }

    /// Get individuals by gender
    #[must_use]
    pub fn by_gender(&self, gender: Gender) -> Vec<Arc<Individual>> {
        self.inner.filter(|individual| individual.gender == gender)
    }

    /// Get individuals by gender string
    #[must_use]
    pub fn by_gender_str(&self, gender: &str) -> Vec<Arc<Individual>> {
        let gender_enum: Gender = gender.into();
        self.by_gender(gender_enum)
    }

    /// Get individuals by municipality
    #[must_use]
    pub fn by_municipality(&self, municipality_code: &str) -> Vec<Arc<Individual>> {
        self.inner.filter(|individual| {
            individual
                .municipality_code
                .as_ref()
                .is_some_and(|code| code == municipality_code)
        })
    }

    /// Get individuals with a specific relationship (mother/father/child)
    #[must_use]
    pub fn with_relationship(
        &self,
        relationship_type: &str,
        related_pnr: &str,
    ) -> Vec<Arc<Individual>> {
        match relationship_type {
            "mother" => self.filter(|ind| {
                ind.mother_pnr
                    .as_ref()
                    .is_some_and(|pnr| pnr == related_pnr)
            }),
            "father" => self.filter(|ind| {
                ind.father_pnr
                    .as_ref()
                    .is_some_and(|pnr| pnr == related_pnr)
            }),
            "child" => {
                // Find individuals where this person is their mother or father
                self.filter(|ind| {
                    (ind.mother_pnr
                        .as_ref()
                        .is_some_and(|pnr| pnr == related_pnr))
                        || (ind
                            .father_pnr
                            .as_ref()
                            .is_some_and(|pnr| pnr == related_pnr))
                })
            }
            _ => Vec::new(),
        }
    }

    /// Get the raw collection
    #[must_use]
    pub const fn raw(&self) -> &GenericCollection<Individual> {
        &self.inner
    }

    /// Get a mutable reference to the raw collection
    pub const fn raw_mut(&mut self) -> &mut GenericCollection<Individual> {
        &mut self.inner
    }
}

impl ModelCollection<Individual> for IndividualCollection {
    fn add(&mut self, individual: Individual) {
        self.inner.add(individual);
    }

    fn get(&self, id: &String) -> Option<Arc<Individual>> {
        self.inner.get(id)
    }

    fn all(&self) -> Vec<Arc<Individual>> {
        self.inner.all()
    }

    fn filter<F>(&self, predicate: F) -> Vec<Arc<Individual>>
    where
        F: Fn(&Individual) -> bool,
    {
        self.inner.filter(predicate)
    }

    fn count(&self) -> usize {
        self.inner.count()
    }
}

impl TemporalCollection<Individual> for IndividualCollection {
    // Use the default implementations from the trait
}

impl LookupCollection<Individual> for IndividualCollection {
    // Use the default implementations from the trait
}

// Implement BatchCollection if GenericCollection implements it
impl BatchCollection<Individual> for IndividualCollection
where
    GenericCollection<Individual>: BatchCollection<Individual>,
{
    fn load_from_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.inner.load_from_batch(batch)
    }

    fn update_from_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.inner.update_from_batch(batch)
    }

    fn export_to_batch(&self) -> Result<RecordBatch> {
        self.inner.export_to_batch()
    }
}