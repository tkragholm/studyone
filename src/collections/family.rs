//! Family model collection
//!
//! This module provides a specialized collection implementation for Family models.

use crate::collections::GenericCollection;
use crate::common::traits::{ModelCollection, TemporalCollection, LookupCollection};
use crate::models::child::Child;
use crate::models::family::{Family, FamilySnapshot};
use crate::models::types::FamilyType;
use crate::models::individual::Individual;
use crate::models::parent::Parent;
use crate::models::traits::{TemporalValidity, HealthStatus};
use chrono::NaiveDate;
use std::collections::HashMap;
use std::sync::Arc;

/// Specialized collection for Family models
#[derive(Debug, Default)]
pub struct FamilyCollection {
    /// Base generic collection implementation
    inner: GenericCollection<Family>,
    /// Individuals indexed by PNR
    individuals: HashMap<String, Arc<Individual>>,
}

impl FamilyCollection {
    /// Create a new empty family collection
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: GenericCollection::new(),
            individuals: HashMap::new(),
        }
    }
    
    /// Create a collection from a vector of families
    #[must_use]
    pub fn from_families(families: Vec<Family>) -> Self {
        let mut collection = Self::new();
        for family in families {
            collection.add(family);
        }
        collection
    }
    
    /// Add an individual to the collection
    pub fn add_individual(&mut self, individual: Individual) {
        let individual_arc = Arc::new(individual);
        self.individuals
            .insert(individual_arc.pnr.clone(), individual_arc);
    }
    
    /// Update a family in the collection with a new version
    ///
    /// This replaces the existing family with the given `family_id` with a new version.
    /// Returns true if the family was found and updated, false otherwise.
    pub fn update_family(&mut self, family_id: &str, updated_family: Family) -> bool {
        if self.contains(&family_id.to_string()) {
            self.add(updated_family);
            true
        } else {
            false
        }
    }
    
    /// Update a child in a family
    ///
    /// Finds the family containing the child with the given PNR and updates it with
    /// the modified child. Returns true if the child was found and updated, false otherwise.
    pub fn update_child(&mut self, child_pnr: &str, updated_child: Child) -> bool {
        // Find all families that contain this child
        let mut updated = false;
        
        // We need to collect family_ids first to avoid borrowing issues
        let family_ids: Vec<String> = self.inner.all()
            .iter()
            .filter(|family| {
                family
                    .children
                    .iter()
                    .any(|child| child.individual().pnr == child_pnr)
            })
            .map(|family| family.family_id.clone())
            .collect();
            
        for family_id in family_ids {
            if let Some(family) = self.get(&family_id) {
                // Create a mutable version
                let mut new_family = Family {
                    family_id: family.family_id.clone(),
                    family_type: family.family_type,
                    mother: family.mother.clone(),
                    father: family.father.clone(),
                    children: Vec::new(), // Will be replaced
                    municipality_code: family.municipality_code.clone(),
                    is_rural: family.is_rural,
                    valid_from: family.valid_from,
                    valid_to: family.valid_to,
                    has_parental_comorbidity: family.has_parental_comorbidity,
                    has_support_network: family.has_support_network,
                };
                
                // Create a new children vector with the updated child
                let new_children: Vec<Arc<Child>> = family
                    .children
                    .iter()
                    .map(|child| {
                        if child.individual().pnr == child_pnr {
                            // Replace with updated child
                            Arc::new(updated_child.clone())
                        } else {
                            // Keep the original child
                            child.clone()
                        }
                    })
                    .collect();
                    
                // Replace the children vector
                new_family.children = new_children;
                
                // Update the family in the collection
                self.add(new_family);
                updated = true;
            }
        }
        
        updated
    }
    
    /// Update a parent in a family
    ///
    /// Finds the family containing the parent with the given PNR and updates it with
    /// the modified parent. Returns true if the parent was found and updated, false otherwise.
    pub fn update_parent(&mut self, parent_pnr: &str, updated_parent: Parent) -> bool {
        // Find all families where this person is a parent
        let mut updated = false;
        
        // We need to collect family_ids first to avoid borrowing issues
        let family_ids: Vec<String> = self.inner.all()
            .iter()
            .filter(|family| {
                (family.mother.is_some()
                    && family.mother.as_ref().unwrap().individual().pnr == parent_pnr)
                    || (family.father.is_some()
                        && family.father.as_ref().unwrap().individual().pnr == parent_pnr)
            })
            .map(|family| family.family_id.clone())
            .collect();
            
        for family_id in family_ids {
            if let Some(family) = self.get(&family_id) {
                // Create a mutable version
                let mut new_family = Family {
                    family_id: family.family_id.clone(),
                    family_type: family.family_type,
                    mother: family.mother.clone(),
                    father: family.father.clone(),
                    children: family.children.clone(),
                    municipality_code: family.municipality_code.clone(),
                    is_rural: family.is_rural,
                    valid_from: family.valid_from,
                    valid_to: family.valid_to,
                    has_parental_comorbidity: family.has_parental_comorbidity,
                    has_support_network: family.has_support_network,
                };
                
                // Update mother if PNR matches
                if new_family.mother.is_some()
                    && new_family.mother.as_ref().unwrap().individual().pnr == parent_pnr
                {
                    new_family.mother = Some(Arc::new(updated_parent.clone()));
                    updated = true;
                }
                
                // Update father if PNR matches
                if new_family.father.is_some()
                    && new_family.father.as_ref().unwrap().individual().pnr == parent_pnr
                {
                    new_family.father = Some(Arc::new(updated_parent.clone()));
                    updated = true;
                }
                
                // Update the family in the collection
                if updated {
                    self.add(new_family);
                }
            }
        }
        
        updated
    }
    
    /// Get an individual by their PNR
    #[must_use]
    pub fn get_individual(&self, pnr: &str) -> Option<Arc<Individual>> {
        self.individuals.get(pnr).cloned()
    }
    
    /// Get all individuals in the collection
    #[must_use]
    pub fn get_individuals(&self) -> Vec<Arc<Individual>> {
        self.individuals.values().cloned().collect()
    }
    
    /// Get families with a specific type
    #[must_use]
    pub fn get_families_by_type(&self, family_type: FamilyType) -> Vec<Arc<Family>> {
        self.filter(|family| family.family_type == family_type)
    }
    
    /// Get families valid at a specific date
    #[must_use]
    pub fn get_families_valid_at(&self, date: &NaiveDate) -> Vec<Arc<Family>> {
        self.filter(|family| family.was_valid_at(date))
    }
    
    /// Get family snapshots for all families at a specific date
    #[must_use]
    pub fn get_snapshots_at(&self, date: &NaiveDate) -> Vec<FamilySnapshot> {
        // Get families valid at the date
        let valid_families = self.get_families_valid_at(date);
        
        // Create snapshots for each valid family
        let mut snapshots = Vec::new();
        
        for family in valid_families {
            // Filter for children who were alive and in the family at the given date
            let children: Vec<Arc<Child>> = family
                .children
                .iter()
                .filter(|child| {
                    let individual = child.individual();
                    individual.was_alive_at(date) && individual.was_resident_at(date)
                })
                .cloned()
                .collect();
                
            // Check if mother was present
            let mother_present = family
                .mother
                .as_ref()
                .is_some_and(|m| m.individual().was_resident_at(date));
                
            // Check if father was present
            let father_present = family
                .father
                .as_ref()
                .is_some_and(|f| f.individual().was_resident_at(date));
                
            // Determine the effective family type based on parents present
            let effective_type = match (mother_present, father_present) {
                (true, true) => FamilyType::TwoParent,
                (true, false) => FamilyType::SingleMother,
                (false, true) => FamilyType::SingleFather,
                (false, false) => FamilyType::NoParent,
            };
            
            // Create the snapshot
            let snapshot = FamilySnapshot {
                family_id: family.family_id.clone(),
                family_type: effective_type,
                mother: if mother_present {
                    family.mother.clone()
                } else {
                    None
                },
                father: if father_present {
                    family.father.clone()
                } else {
                    None
                },
                children,
                municipality_code: family.municipality_code.clone(),
                is_rural: family.is_rural,
                snapshot_date: *date,
                has_parental_comorbidity: (mother_present
                    && family.mother.as_ref().unwrap().had_diagnosis_before(date))
                    || (father_present
                        && family.father.as_ref().unwrap().had_diagnosis_before(date)),
                has_support_network: family.has_support_network,
            };
            
            snapshots.push(snapshot);
        }
        
        snapshots
    }
    
    /// Get case families (families with a child with SCD) at a specific date
    #[must_use]
    pub fn get_case_families_at(&self, date: &NaiveDate) -> Vec<FamilySnapshot> {
        self.get_snapshots_at(date)
            .into_iter()
            .filter(FamilySnapshot::is_eligible_case)
            .collect()
    }
    
    /// Get control families (families without a child with SCD) at a specific date
    #[must_use]
    pub fn get_control_families_at(&self, date: &NaiveDate) -> Vec<FamilySnapshot> {
        self.get_snapshots_at(date)
            .into_iter()
            .filter(FamilySnapshot::is_eligible_control)
            .collect()
    }
    
    /// Get the raw collection
    #[must_use]
    pub const fn raw(&self) -> &GenericCollection<Family> {
        &self.inner
    }
    
    /// Get a mutable reference to the raw collection
    pub const fn raw_mut(&mut self) -> &mut GenericCollection<Family> {
        &mut self.inner
    }
}

impl ModelCollection<Family> for FamilyCollection {
    fn add(&mut self, family: Family) {
        self.inner.add(family);
    }
    
    fn get(&self, id: &String) -> Option<Arc<Family>> {
        self.inner.get(id)
    }
    
    fn all(&self) -> Vec<Arc<Family>> {
        self.inner.all()
    }
    
    fn filter<F>(&self, predicate: F) -> Vec<Arc<Family>>
    where
        F: Fn(&Family) -> bool,
    {
        self.inner.filter(predicate)
    }
    
    fn count(&self) -> usize {
        self.inner.count()
    }
}

impl TemporalCollection<Family> for FamilyCollection {
    // Use the default implementations from the trait
}

impl LookupCollection<Family> for FamilyCollection {
    // Use the default implementations from the trait
}