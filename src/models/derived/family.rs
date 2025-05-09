//! Family unit representation
//!
//! This module contains the Family model, which represents a family unit in the study.
//! A family consists of parents and children and can be used to analyze combined household
//! income and family-level economic impacts.

use super::child::Child;
use super::parent::Parent;
use crate::error::Result;
use crate::models::collections::ModelCollection;
use crate::models::core::Individual;
use crate::models::core::traits::{ArrowSchema, EntityModel, HealthStatus, TemporalValidity};
use crate::models::core::types::FamilyType;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use std::collections::HashMap;
use std::sync::Arc;

/// Representation of a family unit with temporal validity
#[derive(Debug, Clone)]
pub struct Family {
    /// Unique family identifier
    pub family_id: String,
    /// Type of family
    pub family_type: FamilyType,
    /// Mother in the family (if present)
    pub mother: Option<Arc<Parent>>,
    /// Father in the family (if present)
    pub father: Option<Arc<Parent>>,
    /// Children in the family
    pub children: Vec<Arc<Child>>,
    /// Municipality code at index date
    pub municipality_code: Option<String>,
    /// Whether the family lives in a rural area
    pub is_rural: bool,
    /// Start date of the family's validity in this composition
    pub valid_from: NaiveDate,
    /// End date of the family's validity in this composition (None if still valid)
    pub valid_to: Option<NaiveDate>,
    /// Whether either parent has a documented comorbidity at index date
    pub has_parental_comorbidity: bool,
    /// Whether the family has family support network in same municipality
    pub has_support_network: bool,
}

impl Family {
    /// Create a new family with minimum required information
    #[must_use]
    pub const fn new(family_id: String, family_type: FamilyType, valid_from: NaiveDate) -> Self {
        Self {
            family_id,
            family_type,
            mother: None,
            father: None,
            children: Vec::new(),
            municipality_code: None,
            is_rural: false,
            valid_from,
            valid_to: None,
            has_parental_comorbidity: false,
            has_support_network: false,
        }
    }

    /// Set the mother for this family
    #[must_use]
    pub fn with_mother(mut self, mother: Arc<Parent>) -> Self {
        self.mother = Some(mother);
        self
    }

    /// Set the father for this family
    #[must_use]
    pub fn with_father(mut self, father: Arc<Parent>) -> Self {
        self.father = Some(father);
        self
    }

    /// Add a child to this family
    pub fn add_child(&mut self, child: Arc<Child>) {
        self.children.push(child);
    }

    /// Get number of children in the family
    #[must_use]
    pub fn family_size(&self) -> usize {
        self.children.len()
    }

    /// Check if family has any children with severe chronic disease
    #[must_use]
    pub fn has_child_with_scd(&self) -> bool {
        self.children.iter().any(|child| child.has_scd())
    }

    /// Determine if both parents were present at a specific date
    #[must_use]
    pub fn has_both_parents_at(&self, date: &NaiveDate) -> bool {
        let mother_present = self
            .mother
            .as_ref()
            .is_some_and(|m| m.individual().was_resident_at(date));

        let father_present = self
            .father
            .as_ref()
            .is_some_and(|f| f.individual().was_resident_at(date));

        mother_present && father_present
    }
}

// Implement EntityModel trait
impl EntityModel for Family {
    type Id = String;

    fn id(&self) -> &Self::Id {
        &self.family_id
    }

    fn key(&self) -> String {
        self.family_id.clone()
    }
}

// Implement TemporalValidity trait
impl TemporalValidity for Family {
    fn was_valid_at(&self, date: &NaiveDate) -> bool {
        if self.valid_from > *date {
            return false;
        }

        if let Some(valid_to) = self.valid_to {
            if valid_to < *date {
                return false;
            }
        }

        true
    }

    fn valid_from(&self) -> NaiveDate {
        self.valid_from
    }

    fn valid_to(&self) -> Option<NaiveDate> {
        self.valid_to
    }

    fn snapshot_at(&self, date: &NaiveDate) -> Option<Self> {
        // Return None if family wasn't valid at the specified date
        if !self.was_valid_at(date) {
            return None;
        }

        // Filter for children who were alive and in the family at the given date
        let children: Vec<Arc<Child>> = self
            .children
            .iter()
            .filter(|child| {
                let individual = child.individual();
                individual.was_alive_at(date) && individual.was_resident_at(date)
            })
            .cloned()
            .collect();

        // Check if mother was present
        let mother_present = self
            .mother
            .as_ref()
            .is_some_and(|m| m.individual().was_resident_at(date));

        // Check if father was present
        let father_present = self
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

        // Create a snapshot with filtered information
        let mut family_snapshot = Self {
            family_id: self.family_id.clone(),
            family_type: effective_type,
            mother: if mother_present {
                self.mother.clone()
            } else {
                None
            },
            father: if father_present {
                self.father.clone()
            } else {
                None
            },
            children,
            municipality_code: self.municipality_code.clone(),
            is_rural: self.is_rural,
            valid_from: self.valid_from,
            valid_to: self.valid_to,
            has_parental_comorbidity: self.has_parental_comorbidity,
            has_support_network: self.has_support_network,
        };

        // Update comorbidity status at the snapshot date
        family_snapshot.has_parental_comorbidity = family_snapshot
            .mother
            .as_ref()
            .is_some_and(|m| m.had_diagnosis_before(date))
            || family_snapshot
                .father
                .as_ref()
                .is_some_and(|f| f.had_diagnosis_before(date));

        Some(family_snapshot)
    }
}

// Implement ArrowSchema trait
impl ArrowSchema for Family {
    /// Get the Arrow schema for Family records
    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("family_id", DataType::Utf8, false),
            Field::new("family_type", DataType::Int32, false),
            Field::new("mother_pnr", DataType::Utf8, true),
            Field::new("father_pnr", DataType::Utf8, true),
            Field::new("child_count", DataType::Int32, false),
            Field::new("municipality_code", DataType::Utf8, true),
            Field::new("is_rural", DataType::Boolean, false),
            Field::new("valid_from", DataType::Date32, false),
            Field::new("valid_to", DataType::Date32, true),
            Field::new("has_parental_comorbidity", DataType::Boolean, false),
            Field::new("has_support_network", DataType::Boolean, false),
            Field::new("has_child_with_scd", DataType::Boolean, false),
        ])
    }

    fn from_record_batch(_batch: &RecordBatch) -> Result<Vec<Self>> {
        // This would require having Individual, Parent, and Child objects available
        unimplemented!("Conversion from RecordBatch to Family requires complex composition")
    }

    fn to_record_batch(_families: &[Self]) -> Result<RecordBatch> {
        // Implementation of conversion to RecordBatch
        unimplemented!("Conversion to RecordBatch not yet implemented")
    }
}

/// A snapshot of a family at a specific point in time
#[derive(Debug, Clone)]
pub struct FamilySnapshot {
    /// Family identifier
    pub family_id: String,
    /// Type of family at the snapshot date
    pub family_type: FamilyType,
    /// Mother in the family at snapshot date (if present)
    pub mother: Option<Arc<Parent>>,
    /// Father in the family at snapshot date (if present)
    pub father: Option<Arc<Parent>>,
    /// Children in the family at snapshot date
    pub children: Vec<Arc<Child>>,
    /// Municipality code at snapshot date
    pub municipality_code: Option<String>,
    /// Whether the family lived in a rural area at snapshot date
    pub is_rural: bool,
    /// Date of the snapshot
    pub snapshot_date: NaiveDate,
    /// Whether either parent had a documented comorbidity at snapshot date
    pub has_parental_comorbidity: bool,
    /// Whether the family had family support network at snapshot date
    pub has_support_network: bool,
}

impl FamilySnapshot {
    /// Get number of children in the family at snapshot date
    #[must_use]
    pub fn family_size(&self) -> usize {
        self.children.len()
    }

    /// Check if family had any children with severe chronic disease at snapshot date
    #[must_use]
    pub fn has_child_with_scd(&self) -> bool {
        self.children
            .iter()
            .any(|child| child.had_scd_at(&self.snapshot_date))
    }

    /// Check if the family can be a case family (has child with SCD)
    #[must_use]
    pub fn is_eligible_case(&self) -> bool {
        self.has_child_with_scd()
    }

    /// Check if the family can be a control family (no child with SCD)
    #[must_use]
    pub fn is_eligible_control(&self) -> bool {
        !self.has_child_with_scd()
    }
}

// Implement EntityModel trait for FamilySnapshot
impl EntityModel for FamilySnapshot {
    type Id = String;

    fn id(&self) -> &Self::Id {
        &self.family_id
    }

    fn key(&self) -> String {
        format!("{}:{}", self.family_id, self.snapshot_date)
    }
}

/// A collection of families that can be efficiently queried
#[derive(Debug, Clone, Default)]
pub struct FamilyCollection {
    /// Families indexed by `family_id`
    families: HashMap<String, Arc<Family>>,
    /// Individuals indexed by PNR
    individuals: HashMap<String, Arc<Individual>>,
}

impl FamilyCollection {
    /// Create a new empty `FamilyCollection`
    #[must_use]
    pub fn new() -> Self {
        Self {
            families: HashMap::new(),
            individuals: HashMap::new(),
        }
    }

    /// Add a family to the collection
    pub fn add_family(&mut self, family: Family) {
        let family_arc = Arc::new(family);
        let family_id = family_arc.family_id.clone();

        // Add family to the main index
        self.families.insert(family_id, family_arc.clone());

        // Add individuals to the PNR index
        if let Some(ref mother) = family_arc.mother {
            self.individuals.insert(mother.individual().pnr.clone(), Arc::new(mother.individual().clone()));
        }

        if let Some(ref father) = family_arc.father {
            self.individuals.insert(father.individual().pnr.clone(), Arc::new(father.individual().clone()));
        }

        for child in &family_arc.children {
            self.individuals
                .insert(child.individual().pnr.clone(), Arc::new(child.individual().clone()));
        }
    }

    /// Get the count of individuals in the collection
    #[must_use] pub fn individual_count(&self) -> usize {
        self.individuals.len()
    }

    /// Get the count of families in the collection
    #[must_use] pub fn family_count(&self) -> usize {
        self.families.len()
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
        if self.families.contains_key(family_id) {
            let family_arc = Arc::new(updated_family);
            self.families.insert(family_id.to_string(), family_arc);
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
        let family_ids: Vec<String> = self
            .families
            .values()
            .filter(|family| {
                family
                    .children
                    .iter()
                    .any(|child| child.individual().pnr == child_pnr)
            })
            .map(|family| family.family_id.clone())
            .collect();

        for family_id in family_ids {
            if let Some(family) = self.families.get(&family_id).cloned() {
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
                self.families
                    .insert(family_id.clone(), Arc::new(new_family));
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
        let family_ids: Vec<String> = self
            .families
            .values()
            .filter(|family| {
                (family.mother.is_some()
                    && family.mother.as_ref().unwrap().individual().pnr == parent_pnr)
                    || (family.father.is_some()
                        && family.father.as_ref().unwrap().individual().pnr == parent_pnr)
            })
            .map(|family| family.family_id.clone())
            .collect();

        for family_id in family_ids {
            if let Some(family) = self.families.get(&family_id).cloned() {
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
                    self.families
                        .insert(family_id.clone(), Arc::new(new_family));
                }
            }
        }

        updated
    }

    /// Get a family by its ID
    #[must_use]
    pub fn get_family(&self, family_id: &str) -> Option<Arc<Family>> {
        self.families.get(family_id).cloned()
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
}

// Implement ModelCollection trait
impl ModelCollection<Family> for FamilyCollection {
    fn add(&mut self, family: Family) {
        let family_arc = Arc::new(family);
        self.families
            .insert(family_arc.family_id.clone(), family_arc);
    }

    fn get(&self, id: &String) -> Option<Arc<Family>> {
        self.families.get(id).cloned()
    }

    fn all(&self) -> Vec<Arc<Family>> {
        self.families.values().cloned().collect()
    }

    fn filter<F>(&self, predicate: F) -> Vec<Arc<Family>>
    where
        F: Fn(&Family) -> bool,
    {
        self.families
            .values()
            .filter(|family| predicate(family))
            .cloned()
            .collect()
    }

    fn count(&self) -> usize {
        self.families.len()
    }
}
