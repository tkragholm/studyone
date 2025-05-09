//! Population builder implementation
//!
//! This module provides the builder pattern for constructing
//! population datasets step by step.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use chrono::{Datelike, NaiveDate};

use crate::error::Result;
use crate::models::core::traits::HealthStatus;
use crate::models::core::traits::TemporalValidity;
use crate::models::family::FamilyCollection;
use crate::models::{Child, Family, Individual, Parent};
use crate::registry::BefCombinedRegister;
use crate::registry::{MfrChildRegister, RegisterLoader};
use crate::utils::test_utils::{ensure_path_exists, registry_dir};

use super::config::PopulationConfig;
use super::statistics::{PopulationStatistics, PopulationStats};

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
    #[must_use]
    pub fn new(config: PopulationConfig) -> Self {
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

    /// Generate a complete study population using all available registry data
    ///
    /// This method provides a high-level interface for creating a population with:
    /// - Demographic data (BEF, MFR)
    /// - Migration and mortality information (VNDS, DOD)
    /// - Income data (IND)
    /// - Health diagnoses for SCD classification (LPR)
    pub fn generate_from_registries(
        config: PopulationConfig,
        registry_paths: &HashMap<&str, &Path>,
        pnr_filter: Option<HashSet<String>>,
    ) -> Result<Self> {
        use crate::algorithm::population::registry_loader::RegistryIntegration;

        log::info!("Starting population generation from registry data");
        log::info!("Configuration: {config}");

        // Initialize registry integration manager
        let mut integration = RegistryIntegration::new();

        // Track progress
        let mut progress = 0;
        let total_steps = 7; // Total number of steps in the process

        // Step 1: Add demographic data from BEF
        if let Some(bef_path) = registry_paths.get("bef") {
            log::info!(
                "[Step {}/{}] Loading demographic data from BEF registry",
                progress + 1,
                total_steps
            );

            integration.add_demographic_data(bef_path, pnr_filter.as_ref())?;
            progress += 1;
        }

        // Step 2: Add child-specific data from MFR
        if let Some(mfr_path) = registry_paths.get("mfr") {
            log::info!(
                "[Step {}/{}] Loading birth details from MFR registry",
                progress + 1,
                total_steps
            );

            integration.add_child_data(mfr_path, pnr_filter.as_ref())?;
            progress += 1;
        }

        // Step 3: Add migration data from VNDS
        if let Some(vnds_path) = registry_paths.get("vnds") {
            log::info!(
                "[Step {}/{}] Loading migration status from VNDS registry",
                progress + 1,
                total_steps
            );

            integration.add_migration_data(vnds_path, pnr_filter.as_ref())?;
            progress += 1;
        }

        // Step 4: Add mortality data from DOD
        if let Some(dod_path) = registry_paths.get("dod") {
            log::info!(
                "[Step {}/{}] Loading mortality status from DOD registry",
                progress + 1,
                total_steps
            );

            integration.add_mortality_data(dod_path, pnr_filter.as_ref())?;
            progress += 1;
        }

        // Step 5: Add diagnosis data from LPR
        if let Some(lpr_path) = registry_paths.get("lpr") {
            log::info!(
                "[Step {}/{}] Loading diagnosis data from LPR registry",
                progress + 1,
                total_steps
            );

            integration.add_diagnosis_data(lpr_path, pnr_filter.as_ref())?;
            progress += 1;
        }

        // Step 6: Add income data from IND
        if let Some(ind_path) = registry_paths.get("ind") {
            log::info!(
                "[Step {}/{}] Loading income data from IND registry",
                progress + 1,
                total_steps
            );

            // Use the current year as an example - in a real implementation,
            // we would load data for multiple years
            let current_year = chrono::Utc::now().year();
            integration.add_income_data(ind_path, current_year, pnr_filter.as_ref())?;
            progress += 1;
            log::debug!("Completed {progress}/{total_steps} steps of population generation");
        }

        // Step 7: Process and enhance the population
        log::info!("[Step {total_steps}/{total_steps}] Processing population relationships");

        // Identify siblings
        integration.identify_siblings()?;

        // Link diagnoses to children for SCD classification
        integration.link_diagnoses_to_children()?;

        // Link income data to parents
        integration.link_income_to_parents()?;

        // Create final population from the integration result
        let mut population = Self::new(config);
        population.collection = integration.collection().clone();

        // Update statistics
        population.calculate_statistics();

        log::info!("Population generation complete");
        log::info!("{}", population.print_summary());

        Ok(population)
    }

    /// Calculate summary statistics for the population
    pub fn calculate_statistics(&mut self) {
        // Use the PopulationStatistics utilities
        let stats =
            PopulationStatistics::calculate_basic_stats(&self.collection, &self.config.index_date);

        self.individual_count = stats.individual_count;
        self.family_count = stats.family_count;
        self.child_count = stats.child_count;
        self.two_parent_family_count = stats.two_parent_family_count;
        self.scd_family_count = stats.scd_family_count;
    }

    /// Get eligible case families at the index date
    #[must_use]
    pub fn get_case_families(&self) -> Vec<crate::models::family::FamilySnapshot> {
        self.collection
            .get_case_families_at(&self.config.index_date)
    }

    /// Get eligible control families at the index date
    #[must_use]
    pub fn get_control_families(&self) -> Vec<crate::models::family::FamilySnapshot> {
        self.collection
            .get_control_families_at(&self.config.index_date)
    }

    /// Print a summary of the population
    #[must_use]
    pub fn print_summary(&self) -> String {
        let case_families = self.get_case_families();
        let control_families = self.get_control_families();

        let stats = PopulationStats {
            individual_count: self.individual_count,
            family_count: self.family_count,
            child_count: self.child_count,
            two_parent_family_count: self.two_parent_family_count,
            scd_family_count: self.scd_family_count,
        };

        PopulationStatistics::generate_summary(
            &stats,
            &self.config.index_date,
            &case_families,
            &control_families,
        )
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
    #[must_use]
    pub fn new() -> Self {
        Self {
            config: PopulationConfig::default(),
            individuals: HashMap::new(),
            families: HashMap::new(),
            children: HashMap::new(),
            parents: HashMap::new(),
            pnr_filter: None,
        }
    }

    /// Create a new `PopulationBuilder` with progress tracking
    ///
    /// This constructor sets up a builder that will log progress at each step
    #[must_use]
    pub fn with_progress() -> Self {
        log::info!("Initializing population builder with progress tracking");
        Self::new()
    }

    /// Set the population configuration
    #[must_use]
    pub const fn with_config(mut self, config: PopulationConfig) -> Self {
        self.config = config;
        self
    }

    /// Set the PNR filter to limit the population to specific individuals
    #[must_use]
    pub fn with_pnr_filter(mut self, pnr_filter: HashSet<String>) -> Self {
        self.pnr_filter = Some(pnr_filter);
        self
    }

    /// Add BEF (population registry) data to the population
    pub fn add_bef_data(mut self, register: &dyn RegisterLoader, path: &Path) -> Result<Self> {
        log::info!("Loading BEF data from {path:?}");

        // Load the BEF data using the register loader
        let batches = register.load(path, self.pnr_filter.as_ref())?;

        // Process each batch
        for batch in batches {
            // Use the BEF adapter to extract individuals and families
            let (mut batch_individuals, batch_families) =
                BefCombinedRegister::process_batch(&batch)?;

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
    pub fn add_mfr_data(mut self, register: &dyn RegisterLoader, path: &Path) -> Result<Self> {
        log::info!("Loading MFR data from {path:?}");

        // Load the MFR data using the register loader
        let batches = register.load(path, self.pnr_filter.as_ref())?;

        // Create individual lookup for MFR adapter
        let individual_lookup: HashMap<String, Arc<Individual>> = self
            .individuals
            .iter()
            .map(|(k, v)| (k.clone(), Arc::new(v.clone())))
            .collect();

        // Create MFR adapter with individual lookup
        let adapter = MfrChildRegister::new_with_lookup(individual_lookup);

        // Process each batch using the adapter's process_batch method
        for batch in batches {
            // Use the adapter instance to extract child data
            let child_details = adapter.process_batch(&batch)?;

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
                    child = child.with_birth_details(birth_weight, gestational_age, apgar_score);

                    // Store the enriched child object
                    self.children.insert(child.individual().pnr.clone(), child);
                }
            }
        }

        log::info!("Added {} children from MFR data", self.children.len());

        Ok(self)
    }

    /// Identify parents and children based on family relationships
    #[must_use]
    pub fn identify_family_roles(mut self) -> Self {
        log::info!(
            "Identifying family roles for {} individuals in {} families",
            self.individuals.len(),
            self.families.len()
        );

        // Log available family data before processing
        log::info!(
            "Identifying family roles from {} individuals",
            self.individuals.len()
        );

        // Extract all family IDs from individuals
        let mut family_ids = HashSet::new();
        for individual in self.individuals.values() {
            if let Some(family_id) = &individual.family_id {
                family_ids.insert(family_id.clone());
            }
        }
        log::info!(
            "Found {} unique family IDs in individual data",
            family_ids.len()
        );

        // Extract family relationships from BEF data
        let relationships = BefCombinedRegister::extract_relationships(
            &self.individuals.values().cloned().collect::<Vec<_>>(),
        );

        log::info!("Extracted {} family relationships", relationships.len());

        // Process each family
        for (family_id, (mother_pnr, father_pnr, children_pnrs)) in relationships {
            // Find all relevant individuals
            let mut parent_pnrs = HashSet::new();

            // Add mother if present and exists in our individuals
            if let Some(mother_pnr) = &mother_pnr {
                if self.individuals.contains_key(mother_pnr) {
                    parent_pnrs.insert(mother_pnr.clone());
                    log::debug!("Adding mother {mother_pnr} to family {family_id}");
                }
            }

            // Add father if present and exists in our individuals
            if let Some(father_pnr) = &father_pnr {
                if self.individuals.contains_key(father_pnr) {
                    parent_pnrs.insert(father_pnr.clone());
                    log::debug!("Adding father {father_pnr} to family {family_id}");
                }
            }

            // Create parent objects for identified parents
            for pnr in &parent_pnrs {
                if let Some(individual) = self.individuals.get(&pnr.to_string()) {
                    // Create a new Parent if it doesn't already exist
                    if !self.parents.contains_key(&pnr.to_string()) {
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
    #[must_use]
    pub fn build(mut self) -> Population {
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

            // Log family composition for debugging
            log::debug!(
                "Family {}: mother={}, father={}, children={}",
                family_id,
                has_mother,
                has_father,
                family.children.len()
            );

            // Add family if it meets criteria
            if has_children && (!self.config.two_parent_only || (has_mother && has_father)) {
                collection.add_family(family);
            } else {
                log::debug!(
                    "Skipping family {} (children={}, two_parent_only={}, has_mother={}, has_father={})",
                    family_id,
                    has_children,
                    self.config.two_parent_only,
                    has_mother,
                    has_father
                );
            }
        }

        // Create the population with the collection
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

/// Generate a test population from the test data
///
/// This is a utility function to easily create a test population
/// for development and testing purposes using test data directories.
pub fn generate_test_population() -> Result<Population> {
    use crate::registry::factory;

    // Create a population configuration
    let config = PopulationConfig {
        index_date: NaiveDate::from_ymd_opt(2018, 1, 1).unwrap(),
        min_age: Some(0),
        max_age: Some(18),
        resident_only: true,
        two_parent_only: false,
        study_start_date: Some(NaiveDate::from_ymd_opt(2000, 1, 1).unwrap()),
        study_end_date: Some(NaiveDate::from_ymd_opt(2022, 12, 31).unwrap()),
    };

    // Initialize the population builder
    let builder = PopulationBuilder::new().with_config(config);

    // Create registry loaders
    let bef_registry = factory::registry_from_name("bef")?;
    let mfr_registry = factory::registry_from_name("mfr")?;

    // Get paths to the test data
    let bef_path = registry_dir("bef");
    let mfr_path = registry_dir("mfr");

    ensure_path_exists(&bef_path)?;
    ensure_path_exists(&mfr_path)?;

    // Build population step by step
    let builder = builder
        .add_bef_data(&*bef_registry, &bef_path)?
        .add_mfr_data(&*mfr_registry, &mfr_path)?
        .identify_family_roles();

    // Create the final population
    let mut population = builder.build();

    // Calculate statistics
    population.calculate_statistics();

    Ok(population)
}
