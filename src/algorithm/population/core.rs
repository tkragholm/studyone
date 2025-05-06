//! Core population generation logic
//!
//! This module contains the central population generation functionality,
//! which builds a study population from demographic and registry data.

use chrono::NaiveDate;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use crate::error::Result;
use crate::models::adapters::RegistryAdapter;
use crate::models::adapters::{BefCombinedAdapter, MfrChildAdapter};
use crate::models::family::FamilyCollection;
use crate::models::family::FamilySnapshot;
use crate::models::{Child, Family, Individual, Parent};
use crate::registry::RegisterLoader;

/// Configuration for population generation
#[derive(Debug, Clone)]
pub struct PopulationConfig {
    /// Index date for the study (defines the point in time for assessment)
    pub index_date: NaiveDate,
    /// Minimum age for inclusion in the study population
    pub min_age: Option<u32>,
    /// Maximum age for inclusion in the study population
    pub max_age: Option<u32>,
    /// Whether to include only individuals resident in Denmark at the index date
    pub resident_only: bool,
    /// Whether to include only families with both parents
    pub two_parent_only: bool,
    /// Start date of the study period (for longitudinal data)
    pub study_start_date: Option<NaiveDate>,
    /// End date of the study period (for longitudinal data)
    pub study_end_date: Option<NaiveDate>,
}

impl Default for PopulationConfig {
    fn default() -> Self {
        Self {
            index_date: NaiveDate::from_ymd_opt(2015, 1, 1).unwrap(),
            min_age: None,
            max_age: None,
            resident_only: true,
            two_parent_only: false,
            study_start_date: None,
            study_end_date: None,
        }
    }
}

impl fmt::Display for PopulationConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Population Configuration:")?;
        writeln!(f, "  Index Date: {}", self.index_date)?;
        if let Some(min_age) = self.min_age {
            writeln!(f, "  Minimum Age: {min_age}")?;
        }
        if let Some(max_age) = self.max_age {
            writeln!(f, "  Maximum Age: {max_age}")?;
        }
        writeln!(f, "  Resident Only: {}", self.resident_only)?;
        writeln!(f, "  Two Parent Only: {}", self.two_parent_only)?;
        if let Some(start) = self.study_start_date {
            writeln!(f, "  Study Start Date: {start}")?;
        }
        if let Some(end) = self.study_end_date {
            writeln!(f, "  Study End Date: {end}")?;
        }
        Ok(())
    }
}

/// Central population structure for a research study
#[derive(Debug)]
pub struct Population {
    /// Configuration used to generate this population
    pub config: PopulationConfig,
    /// Collection of families and individuals in the study population
    pub collection: FamilyCollection,
    /// Total number of individuals in the population
    pub individual_count: usize,
    /// Total number of families in the population
    pub family_count: usize,
    /// Number of children in the population
    pub child_count: usize,
    /// Number of families with both parents present
    pub two_parent_family_count: usize,
    /// Number of families with severe chronic disease
    pub scd_family_count: usize,
}

impl Population {
    /// Create a new population with the specified configuration
    #[must_use] pub fn new(config: PopulationConfig) -> Self {
        Self {
            config,
            collection: FamilyCollection::new(),
            individual_count: 0,
            family_count: 0,
            child_count: 0,
            two_parent_family_count: 0,
            scd_family_count: 0,
        }
    }

    /// Calculate summary statistics for the population
    pub fn calculate_statistics(&mut self) {
        // Update basic counts
        self.individual_count = self.collection.individual_count();
        self.family_count = self.collection.family_count();

        // Calculate more detailed statistics
        let snapshots = self.collection.get_snapshots_at(&self.config.index_date);

        self.child_count = snapshots.iter().map(|s| s.children.len()).sum();

        self.two_parent_family_count = snapshots
            .iter()
            .filter(|s| s.mother.is_some() && s.father.is_some())
            .count();

        self.scd_family_count = snapshots.iter().filter(|s| s.has_child_with_scd()).count();
    }

    /// Get eligible case families at the index date
    #[must_use] pub fn get_case_families(&self) -> Vec<FamilySnapshot> {
        self.collection
            .get_case_families_at(&self.config.index_date)
    }

    /// Get eligible control families at the index date
    #[must_use] pub fn get_control_families(&self) -> Vec<FamilySnapshot> {
        self.collection
            .get_control_families_at(&self.config.index_date)
    }

    /// Print a summary of the population
    #[must_use] pub fn print_summary(&self) -> String {
        let mut summary = String::new();
        summary.push_str("Study Population Summary:\n");
        summary.push_str(&format!("  Index Date: {}\n", self.config.index_date));
        summary.push_str(&format!("  Total Individuals: {}\n", self.individual_count));
        summary.push_str(&format!("  Total Families: {}\n", self.family_count));
        summary.push_str(&format!("  Total Children: {}\n", self.child_count));
        summary.push_str(&format!(
            "  Two-Parent Families: {}\n",
            self.two_parent_family_count
        ));
        summary.push_str(&format!("  Families with SCD: {}\n", self.scd_family_count));

        // Calculate eligibility counts
        let case_count = self.get_case_families().len();
        let control_count = self.get_control_families().len();
        summary.push_str(&format!("  Eligible Case Families: {case_count}\n"));
        summary.push_str(&format!("  Eligible Control Families: {control_count}\n"));

        summary
    }
}

/// Builder for constructing a population step by step
pub struct PopulationBuilder {
    /// Configuration for the population being built
    config: PopulationConfig,
    /// Individual data with PNR as key
    individuals: HashMap<String, Individual>,
    /// Family data with `family_id` as key
    families: HashMap<String, Family>,
    /// Child data with PNR as key
    children: HashMap<String, Child>,
    /// Parent data with PNR as key
    parents: HashMap<String, Parent>,
    /// Set of PNRs to filter individuals (if specified)
    pnr_filter: Option<HashSet<String>>,
}

impl PopulationBuilder {
    /// Create a new population builder with default configuration
    #[must_use] pub fn new() -> Self {
        Self {
            config: PopulationConfig::default(),
            individuals: HashMap::new(),
            families: HashMap::new(),
            children: HashMap::new(),
            parents: HashMap::new(),
            pnr_filter: None,
        }
    }

    /// Set the population configuration
    #[must_use] pub fn with_config(mut self, config: PopulationConfig) -> Self {
        self.config = config;
        self
    }

    /// Set the PNR filter to limit the population to specific individuals
    #[must_use] pub fn with_pnr_filter(mut self, pnr_filter: HashSet<String>) -> Self {
        self.pnr_filter = Some(pnr_filter);
        self
    }

    /// Add BEF (population registry) data to the population
    pub fn add_bef_data(
        mut self,
        register: &dyn RegisterLoader,
        path: &std::path::Path,
    ) -> Result<Self> {
        log::info!("Loading BEF data from {path:?}");

        // Load the BEF data using the register loader
        let batches = register.load(path, self.pnr_filter.as_ref())?;

        // Process each batch
        for batch in batches {
            // Use the BEF adapter to extract individuals and families
            let (mut batch_individuals, batch_families) =
                BefCombinedAdapter::process_batch(&batch)?;

            // Apply filtering based on configuration
            if self.config.resident_only {
                batch_individuals.retain(|i| i.was_resident_at(&self.config.index_date));
            }

            if let Some(min_age) = self.config.min_age {
                batch_individuals.retain(|i| {
                    if let Some(age) = i.age_at(&self.config.index_date) {
                        age >= min_age as i32
                    } else {
                        false // Exclude individuals with unknown age
                    }
                });
            }

            if let Some(max_age) = self.config.max_age {
                batch_individuals.retain(|i| {
                    if let Some(age) = i.age_at(&self.config.index_date) {
                        age <= max_age as i32
                    } else {
                        true // Keep individuals with unknown age (will be filtered elsewhere)
                    }
                });
            }

            // Add filtered individuals to our collection
            for individual in batch_individuals {
                self.individuals.insert(individual.pnr.clone(), individual);
            }

            // Add families to our collection
            for family in batch_families {
                self.families.insert(family.family_id.clone(), family);
            }
        }

        log::info!(
            "Added {} individuals and {} families from BEF data",
            self.individuals.len(),
            self.families.len()
        );

        Ok(self)
    }

    /// Add MFR (birth registry) data to the population to enrich child information
    pub fn add_mfr_data(
        mut self,
        register: &dyn RegisterLoader,
        path: &std::path::Path,
    ) -> Result<Self> {
        log::info!("Loading MFR data from {path:?}");

        // Load the MFR data using the register loader
        let batches = register.load(path, self.pnr_filter.as_ref())?;

        // Create individual lookup for MFR adapter
        let individual_lookup: HashMap<String, Arc<Individual>> = self
            .individuals
            .iter()
            .map(|(k, v)| (k.clone(), Arc::new(v.clone())))
            .collect();

        // Create MFR adapter 
        // Using underscore to avoid unused variable warning since we're using the static function
        let _adapter = MfrChildAdapter::new(individual_lookup);

        // Process each batch using the static function
        for batch in batches {
            // Use the RegistryAdapter trait implementation to extract child data
            let child_details = MfrChildAdapter::from_record_batch(&batch)?;

            // For each child detail record, try to find the corresponding individual
            for detail in child_details {
                // Get the individual PNR from the child's individual reference
                let pnr = detail.individual().pnr.clone();
                if let Some(individual) = self.individuals.get(&pnr) {
                    // Create a Child object using individual
                    let individual_arc = Arc::new(individual.clone());
                    let mut child = Child::from_individual(individual_arc);

                    // Enrich with MFR-specific fields - combine all details in one call
                    let birth_weight = detail.birth_weight;
                    let gestational_age = detail.gestational_age;
                    let apgar_score = detail.apgar_score;
                    
                    // Set all birth details at once to avoid moved value errors
                    child = child.with_birth_details(
                        birth_weight,
                        gestational_age,
                        apgar_score
                    );

                    // Store the enriched child object
                    self.children.insert(child.individual().pnr.clone(), child);
                }
            }
        }

        log::info!("Added {} children from MFR data", self.children.len());

        Ok(self)
    }

    /// Identify parents and children based on family relationships
    #[must_use] pub fn identify_family_roles(mut self) -> Self {
        log::info!(
            "Identifying family roles for {} individuals in {} families",
            self.individuals.len(),
            self.families.len()
        );

        // Extract family relationships from BEF data
        let relationships = BefCombinedAdapter::extract_relationships(
            &self.individuals.values().cloned().collect::<Vec<_>>(),
        );

        // Process each family
        for (_family_id, (mother_pnr, father_pnr, children_pnrs)) in relationships {
            // Find all relevant individuals
            let mut parent_pnrs = HashSet::new();

            // Add mother if present and exists in our individuals
            if let Some(mother_pnr) = &mother_pnr {
                if self.individuals.contains_key(mother_pnr) {
                    parent_pnrs.insert(mother_pnr.clone());
                }
            }

            // Add father if present and exists in our individuals
            if let Some(father_pnr) = &father_pnr {
                if self.individuals.contains_key(father_pnr) {
                    parent_pnrs.insert(father_pnr.clone());
                }
            }

            // Create parent objects for identified parents
            for pnr in &parent_pnrs {
                if let Some(individual) = self.individuals.get(pnr) {
                    // Create a new Parent if it doesn't already exist
                    if !self.parents.contains_key(pnr) {
                        let individual_arc = Arc::new(individual.clone());
                        let parent = Parent::from_individual(individual_arc);
                        self.parents.insert(pnr.clone(), parent);
                    }
                }
            }

            // Create child objects for identified children who aren't already parents
            for child_pnr in children_pnrs {
                if self.individuals.contains_key(&child_pnr) && !parent_pnrs.contains(&child_pnr) {
                    // If we already have MFR data for this child, skip (already created)
                    if !self.children.contains_key(&child_pnr) {
                        let individual = self.individuals.get(&child_pnr).unwrap();
                        let individual_arc = Arc::new(individual.clone());
                        let child = Child::from_individual(individual_arc);
                        self.children.insert(child_pnr.clone(), child);
                    }
                }
            }
        }

        log::info!(
            "Identified {} parents and {} children",
            self.parents.len(),
            self.children.len()
        );

        self
    }

    /// Build the final Population object
    #[must_use] pub fn build(mut self) -> Population {
        log::info!(
            "Building population with {} individuals, {} families, {} parents, {} children",
            self.individuals.len(),
            self.families.len(),
            self.parents.len(),
            self.children.len()
        );

        let mut collection = FamilyCollection::new();

        // Add all individuals to the collection
        for individual in self.individuals.values() {
            collection.add_individual(individual.clone());
        }

        // First, collect all family IDs to avoid borrowing issues
        let family_ids: Vec<String> = self.families.keys().cloned().collect();

        // Process each family, adding parent and child references
        for family_id in family_ids {
            // Get a mutable copy of the family
            let mut family = self.families.remove(&family_id).unwrap();

            // Build Arc references to parents and children
            if let Some(mother_pnr) = family.mother.as_ref().map(|m| m.individual().pnr.clone()) {
                if let Some(mother) = self.parents.get(&mother_pnr) {
                    family = family.with_mother(Arc::new(mother.clone()));
                }
            }

            if let Some(father_pnr) = family.father.as_ref().map(|f| f.individual().pnr.clone()) {
                if let Some(father) = self.parents.get(&father_pnr) {
                    family = family.with_father(Arc::new(father.clone()));
                }
            }

            // Get a family snapshot at the index date to determine membership
            if let Some(snapshot) = family.snapshot_at(&self.config.index_date) {
                for child_ref in snapshot.children {
                    let child_pnr = child_ref.individual().pnr.clone();
                    if let Some(child) = self.children.get(&child_pnr) {
                        family.add_child(Arc::new(child.clone()));
                    }
                }
            }

            // Skip families with no members or those that don't meet the two-parent requirement
            let has_mother = family.mother.is_some();
            let has_father = family.father.is_some();
            let has_children = !family.children.is_empty();

            if has_children && (!self.config.two_parent_only || (has_mother && has_father)) {
                collection.add_family(family);
            }
        }

        // Create the population and calculate statistics
        let mut population = Population {
            config: self.config,
            collection,
            individual_count: 0,
            family_count: 0,
            child_count: 0,
            two_parent_family_count: 0,
            scd_family_count: 0,
        };

        // Update statistics
        population.calculate_statistics();

        log::info!(
            "Built population with {} individuals and {} families",
            population.individual_count,
            population.family_count
        );

        population
    }
}

impl Default for PopulationBuilder {
    fn default() -> Self {
        Self::new()
    }
}
