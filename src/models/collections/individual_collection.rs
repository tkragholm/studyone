//! Individual collection implementation
//!
//! This module provides a collection type for storing and querying
//! Individual models efficiently.

use crate::models::collections::collection_traits::ModelCollection;
use crate::models::core::Individual;
use crate::models::core::traits::HealthStatus;
use std::collections::HashMap;
use std::sync::Arc;

/// A collection of individuals that can be efficiently queried
#[derive(Debug, Default)]
pub struct IndividualCollection {
    /// Individuals indexed by PNR
    individuals: HashMap<String, Arc<Individual>>,
}

impl IndividualCollection {
    /// Create a new empty `IndividualCollection`
    #[must_use]
    pub fn new() -> Self {
        Self {
            individuals: HashMap::new(),
        }
    }

    /// Create a new `IndividualCollection` with an initial set of individuals
    #[must_use]
    pub fn with_individuals(individuals: Vec<Individual>) -> Self {
        let mut collection = Self::new();
        for individual in individuals {
            collection.add(individual);
        }
        collection
    }

    /// Get individuals by a specific gender
    #[must_use]
    pub fn by_gender(&self, gender: crate::models::core::types::Gender) -> Vec<Arc<Individual>> {
        self.filter(|individual| individual.gender == gender)
    }

    /// Get individuals that were alive at a specific date
    #[must_use]
    pub fn alive_at(&self, date: &chrono::NaiveDate) -> Vec<Arc<Individual>> {
        self.filter(|individual| individual.was_alive_at(date))
    }

    /// Get individuals that were resident in Denmark at a specific date
    #[must_use]
    pub fn resident_at(&self, date: &chrono::NaiveDate) -> Vec<Arc<Individual>> {
        self.filter(|individual| individual.was_resident_at(date))
    }

    /// Get individuals by municipality code
    #[must_use]
    pub fn by_municipality(&self, municipality_code: &str) -> Vec<Arc<Individual>> {
        self.filter(|individual| {
            individual
                .municipality_code
                .as_ref()
                .map_or(false, |code| code == municipality_code)
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
                    .map_or(false, |pnr| pnr == related_pnr)
            }),
            "father" => self.filter(|ind| {
                ind.father_pnr
                    .as_ref()
                    .map_or(false, |pnr| pnr == related_pnr)
            }),
            "child" => {
                // Find individuals where this person is their mother or father
                self.filter(|ind| {
                    (ind.mother_pnr
                        .as_ref()
                        .map_or(false, |pnr| pnr == related_pnr))
                        || (ind
                            .father_pnr
                            .as_ref()
                            .map_or(false, |pnr| pnr == related_pnr))
                })
            }
            _ => Vec::new(),
        }
    }
}

impl ModelCollection<Individual> for IndividualCollection {
    fn add(&mut self, individual: Individual) {
        let pnr = individual.pnr.clone();
        let individual_arc = Arc::new(individual);
        self.individuals.insert(pnr, individual_arc);
    }

    fn get(&self, id: &String) -> Option<Arc<Individual>> {
        self.individuals.get(id).cloned()
    }

    fn all(&self) -> Vec<Arc<Individual>> {
        self.individuals.values().cloned().collect()
    }

    fn filter<F>(&self, predicate: F) -> Vec<Arc<Individual>>
    where
        F: Fn(&Individual) -> bool,
    {
        self.individuals
            .values()
            .filter(|individual| predicate(individual))
            .cloned()
            .collect()
    }

    fn count(&self) -> usize {
        self.individuals.len()
    }
}
