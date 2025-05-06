//! Family unit representation
//!
//! This module contains the Family model, which represents a family unit in the study.
//! A family consists of parents and children and can be used to analyze combined household
//! income and family-level economic impacts.

use super::child::Child;
use super::individual::Individual;
use super::parent::Parent;
use arrow::datatypes::{DataType, Field, Schema};
use chrono::NaiveDate;
use std::collections::HashMap;
use std::sync::Arc;

/// Type of family based on composition
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FamilyType {
    /// Two-parent family with both parents present
    TwoParent,
    /// Single-parent family with only a mother
    SingleMother,
    /// Single-parent family with only a father
    SingleFather,
    /// No parents present (e.g., children living with other relatives)
    NoParent,
    /// Unknown family type
    Unknown,
}

impl From<i32> for FamilyType {
    fn from(value: i32) -> Self {
        match value {
            1 => FamilyType::TwoParent,
            2 => FamilyType::SingleMother,
            3 => FamilyType::SingleFather,
            4 => FamilyType::NoParent,
            _ => FamilyType::Unknown,
        }
    }
}

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
    pub fn new(family_id: String, family_type: FamilyType, valid_from: NaiveDate) -> Self {
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

    /// Check if this family was valid at a specific date
    #[must_use]
    pub fn was_valid_at(&self, date: &NaiveDate) -> bool {
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

    /// Get the Arrow schema for Family records
    #[must_use]
    pub fn schema() -> Schema {
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

    /// Create a snapshot of the family at a specific point in time
    #[must_use]
    pub fn snapshot_at(&self, date: &NaiveDate) -> Option<FamilySnapshot> {
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

        Some(FamilySnapshot {
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
            snapshot_date: *date,
            has_parental_comorbidity: self.has_parental_comorbidity,
            has_support_network: self.has_support_network,
        })
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

/// A collection of families that can be efficiently queried
#[derive(Debug)]
pub struct FamilyCollection {
    /// Families indexed by `family_id`
    families: HashMap<String, Arc<Family>>,
    /// Individuals indexed by PNR
    individuals: HashMap<String, Arc<Individual>>,
}

impl Default for FamilyCollection {
    fn default() -> Self {
        Self::new()
    }
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
        self.families
            .insert(family_arc.family_id.clone(), family_arc);
    }

    /// Add an individual to the collection
    pub fn add_individual(&mut self, individual: Individual) {
        let individual_arc = Arc::new(individual);
        self.individuals
            .insert(individual_arc.pnr.clone(), individual_arc);
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

    /// Get families with a specific type
    #[must_use]
    pub fn get_families_by_type(&self, family_type: FamilyType) -> Vec<Arc<Family>> {
        self.families
            .values()
            .filter(|family| family.family_type == family_type)
            .cloned()
            .collect()
    }

    /// Get families valid at a specific date
    #[must_use]
    pub fn get_families_valid_at(&self, date: &NaiveDate) -> Vec<Arc<Family>> {
        self.families
            .values()
            .filter(|family| family.was_valid_at(date))
            .cloned()
            .collect()
    }

    /// Get family snapshots for all families at a specific date
    #[must_use]
    pub fn get_snapshots_at(&self, date: &NaiveDate) -> Vec<FamilySnapshot> {
        self.families
            .values()
            .filter_map(|family| family.snapshot_at(date))
            .collect()
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

    /// Count the total number of families in the collection
    #[must_use]
    pub fn family_count(&self) -> usize {
        self.families.len()
    }

    /// Count the total number of individuals in the collection
    #[must_use]
    pub fn individual_count(&self) -> usize {
        self.individuals.len()
    }
}
