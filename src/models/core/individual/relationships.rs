//! Family relationships and derived models
//!
//! This module contains methods for working with family relationships
//! and creating derived models like Child and Parent.

use crate::models::core::individual::Individual;
use crate::models::core::types::FamilyType;
use crate::models::derived::{Child, Family, Parent};
use chrono::NaiveDate;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

impl Individual {
    /// Create a lookup map from PNR to Individual
    #[must_use]
    pub fn create_pnr_lookup(individuals: &[Self]) -> HashMap<String, Self> {
        let mut lookup = HashMap::with_capacity(individuals.len());
        for individual in individuals {
            lookup.insert(individual.pnr.clone(), individual.clone());
        }
        lookup
    }

    /// Determine if this individual is a parent based on relations
    #[must_use]
    pub fn is_parent_in_dataset(&self, all_individuals: &[Self]) -> bool {
        all_individuals.iter().any(|ind| {
            (ind.mother_pnr.as_ref() == Some(&self.pnr))
                || (ind.father_pnr.as_ref() == Some(&self.pnr))
        })
    }

    /// Create a Child model from this Individual
    #[must_use]
    pub fn to_child(&self) -> Child {
        Child::from_individual(Arc::new(self.clone()))
    }

    /// Create a Parent model from this Individual
    #[must_use]
    pub fn to_parent(&self) -> Parent {
        Parent::from_individual(Arc::new(self.clone()))
    }

    /// Group individuals by family ID
    #[must_use]
    pub fn group_by_family(individuals: &[Self]) -> HashMap<String, Vec<&Self>> {
        let mut family_map: HashMap<String, Vec<&Self>> = HashMap::new();

        for individual in individuals {
            if let Some(family_id) = &individual.family_id {
                family_map
                    .entry(family_id.clone())
                    .or_default()
                    .push(individual);
            }
        }

        family_map
    }

    /// Create families from a collection of individuals
    #[must_use]
    pub fn create_families(individuals: &[Self], reference_date: &NaiveDate) -> Vec<Family> {
        let family_groups = Self::group_by_family(individuals);
        let mut families = Vec::new();

        for (family_id, members) in family_groups {
            // Identify family members by role
            let mut mothers = Vec::new();
            let mut fathers = Vec::new();
            let mut children = Vec::new();

            for member in &members {
                if member.is_child(reference_date) {
                    children.push(member);
                } else if member.gender == Some("F".to_string()) {
                    mothers.push(member);
                } else if member.gender == Some("M".to_string()) {
                    fathers.push(member);
                }
            }

            // Determine family type
            let family_type = match (mothers.len(), fathers.len()) {
                (1.., 1..) => FamilyType::TwoParent,
                (1.., 0) => FamilyType::SingleMother,
                (0, 1..) => FamilyType::SingleFather,
                (0, 0) => FamilyType::NoParent,
            };

            // Create family object
            let family = Family::new(family_id, family_type, *reference_date);
            // Additional setup for the family would be needed here

            families.push(family);
        }

        families
    }

    /// Create Child models for all children in the dataset
    #[must_use]
    pub fn create_children(individuals: &[Self], reference_date: &NaiveDate) -> Vec<Child> {
        individuals
            .iter()
            .filter(|ind| ind.is_child(reference_date))
            .map(Self::to_child)
            .collect()
    }

    /// Create Parent models for all parents in the dataset
    #[must_use]
    pub fn create_parents(individuals: &[Self]) -> Vec<Parent> {
        let parent_pnrs: HashSet<&String> = individuals
            .iter()
            .filter_map(|ind| ind.mother_pnr.as_ref())
            .chain(individuals.iter().filter_map(|ind| ind.father_pnr.as_ref()))
            .collect();

        individuals
            .iter()
            .filter(|ind| parent_pnrs.contains(&ind.pnr))
            .map(Self::to_parent)
            .collect()
    }
}
