//! Cross-registry integration logic
//!
//! This module provides functionality for integrating data from multiple
//! registries into a cohesive population dataset.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use crate::error::Result;
use crate::models::adapters::RegistryAdapter;
use crate::models::family::FamilyCollection;
use crate::models::{
    Child, Diagnosis, Income,
    adapters::{
        BefCombinedAdapter, IndIncomeAdapter, Lpr2DiagAdapter, Lpr3DiagnoserAdapter,
        MfrChildAdapter,
    },
};

use crate::Individual;
use crate::registry::factory;

/// Registry integration manager for combining data from multiple sources
pub struct RegistryIntegration {
    /// Collection of individuals and families
    collection: FamilyCollection,
    /// Diagnosis data by individual PNR
    diagnoses: HashMap<String, Vec<Diagnosis>>,
    /// Income data by individual PNR
    incomes: HashMap<String, Vec<Income>>,
}

impl RegistryIntegration {
    /// Create a new registry integration manager
    pub fn new() -> Self {
        Self {
            collection: FamilyCollection::new(),
            diagnoses: HashMap::new(),
            incomes: HashMap::new(),
        }
    }

    /// Get the integrated family collection
    pub fn collection(&self) -> &FamilyCollection {
        &self.collection
    }

    /// Get diagnoses for a specific individual
    pub fn get_diagnoses(&self, pnr: &str) -> Option<&Vec<Diagnosis>> {
        self.diagnoses.get(pnr)
    }

    /// Get income data for a specific individual
    pub fn get_incomes(&self, pnr: &str) -> Option<&Vec<Income>> {
        self.incomes.get(pnr)
    }

    /// Add demographic data from BEF registry
    pub fn add_demographic_data(
        &mut self,
        path: &Path,
        pnr_filter: Option<&HashSet<String>>,
    ) -> Result<()> {
        // Create BEF registry loader
        let registry = factory::registry_from_name("bef")?;

        // Load the batches
        let batches = registry.load(path, pnr_filter)?;

        for batch in batches {
            // Use the BEF adapter to process the batch
            let (individuals, families) = BefCombinedAdapter::process_batch(&batch)?;

            // Add individuals to collection
            for individual in individuals {
                self.collection.add_individual(individual);
            }

            // Add families to collection
            for family in families {
                self.collection.add_family(family);
            }
        }

        Ok(())
    }

    /// Add child-specific data from MFR registry
    pub fn add_child_data(
        &mut self,
        path: &Path,
        pnr_filter: Option<&HashSet<String>>,
    ) -> Result<()> {
        // Create MFR registry loader
        let registry = factory::registry_from_name("mfr")?;

        // Load the batches
        let batches = registry.load(path, pnr_filter)?;

        // Create individual lookup for MFR adapter
        let individual_lookup: HashMap<String, Arc<Individual>> = self
            .collection
            .get_individuals()
            .iter()
            .map(|ind| (ind.pnr.clone(), ind.clone())) // ind is already an Arc<Individual>
            .collect();

        // Create MFR adapter with individual lookup
        let adapter = MfrChildAdapter::new(individual_lookup);

        // Process and match children data
        for batch in batches {
            // Use adapter's process_batch method instead of from_record_batch
            let child_details = adapter.process_batch(&batch)?;

            for detail in child_details {
                if let Some(individual) = self.collection.get_individual(&detail.individual().pnr) {
                    // Create a Child object using from_individual
                    // Use the individual directly without creating a new Arc
                    let mut child = Child::from_individual(individual.clone());

                    // Add MFR-specific details using with_birth_details
                    child = child.with_birth_details(
                        detail.birth_weight,
                        detail.gestational_age,
                        detail.apgar_score,
                    );

                    // TODO: Update the original family with this child information
                    // This would require more complex update logic for existing families
                }
            }
        }

        Ok(())
    }

    /// Add diagnosis data from LPR registry
    pub fn add_diagnosis_data(
        &mut self,
        path: &Path,
        pnr_filter: Option<&HashSet<String>>,
    ) -> Result<()> {
        // Determine if this is LPR2 or LPR3 based on path
        let path_str = path.to_string_lossy().to_lowercase();

        if path_str.contains("lpr3") || path_str.contains("diagnoser") {
            // LPR3 registry
            let registry = factory::registry_from_name("lpr3_diagnoser")?;
            let batches = registry.load(path, pnr_filter)?;

            // Create an empty PNR lookup for the adapter
            let pnr_lookup: HashMap<String, String> = HashMap::new();
            // Using underscore to avoid unused variable warning since we're using the static function
            let _adapter = Lpr3DiagnoserAdapter::new(pnr_lookup);

            for batch in batches {
                // Use static function call instead of method
                let batch_diagnoses = Lpr3DiagnoserAdapter::from_record_batch(&batch)?;

                // Group diagnoses by individual
                for diagnosis in batch_diagnoses {
                    self.diagnoses
                        .entry(diagnosis.individual_pnr.clone())
                        .or_default()
                        .push(diagnosis);
                }
            }
        } else {
            // LPR2 registry (assume LPR diag)
            let registry = factory::registry_from_name("lpr_diag")?;
            let batches = registry.load(path, pnr_filter)?;

            // Create an empty PNR lookup for the adapter
            let pnr_lookup: HashMap<String, String> = HashMap::new();
            // Using underscore to avoid unused variable warning since we're using the static function
            let _adapter = Lpr2DiagAdapter::new(pnr_lookup);

            for batch in batches {
                // Use static function call instead of method
                let batch_diagnoses = Lpr2DiagAdapter::from_record_batch(&batch)?;

                // Group diagnoses by individual
                for diagnosis in batch_diagnoses {
                    self.diagnoses
                        .entry(diagnosis.individual_pnr.clone())
                        .or_default()
                        .push(diagnosis);
                }
            }
        }

        Ok(())
    }

    /// Add income data from IND registry
    pub fn add_income_data(
        &mut self,
        path: &Path,
        year: i32,
        pnr_filter: Option<&HashSet<String>>,
    ) -> Result<()> {
        // Create IND registry loader
        let registry = factory::registry_from_name("ind")?;

        // Load the batches
        let batches = registry.load(path, pnr_filter)?;

        // Create IND adapter for the specific year
        let adapter = IndIncomeAdapter::new_without_cpi(year);

        for batch in batches {
            // There's no static version, so we need to keep using the instance method
            let batch_incomes = adapter.from_record_batch_with_year(&batch)?;

            // Group incomes by individual
            for income in batch_incomes {
                self.incomes
                    .entry(income.individual_pnr.clone())
                    .or_default()
                    .push(income);
            }
        }

        Ok(())
    }

    /// Link diagnosis data to children to identify SCD cases
    pub fn link_diagnoses_to_children(&mut self) -> Result<()> {
        // Count of SCD diagnoses linked
        let mut scd_count = 0;

        // Identify families with children
        let family_ids = self
            .collection
            .get_snapshots_at(&chrono::Utc::now().naive_utc().date())
            .into_iter()
            .map(|snapshot| snapshot.family_id)
            .collect::<Vec<_>>();

        for family_id in family_ids {
            // Get the family
            if let Some(family) = self.collection.get_family(&family_id) {
                // Iterate through children in the family
                for child_ref in &family.children {
                    let child_pnr = &child_ref.individual().pnr;

                    // Check if this child has diagnoses
                    if let Some(diagnoses) = self.diagnoses.get(child_pnr) {
                        // Check for SCD-related diagnoses
                        // NOTE: This is a simplified implementation. In a real system,
                        // you would use the full SCD algorithm.
                        let has_scd = diagnoses.iter().any(|d| {
                            d.diagnosis_code.starts_with("C") || // Cancer
                            d.diagnosis_code.starts_with("D80") || // Immunodeficiency
                            d.diagnosis_code.starts_with("G71") || // Muscular disorders
                            d.diagnosis_code.starts_with("Q") // Congenital malformations
                        });

                        if has_scd {
                            // Clone the Child to modify it
                            let child = (**child_ref).clone();

                            // Update SCD status using with_scd
                            // Using current date as the first SCD date and default values for other params
                            let current_date = chrono::Utc::now().naive_utc().date();
                            use crate::models::child::{
                                DiseaseOrigin, DiseaseSeverity, ScdCategory,
                            };

                            // Set child with SCD status - we're not using this modified child directly,
                            // but in a real implementation we would update the child in the collection
                            let _child_with_scd = child.with_scd(
                                ScdCategory::None, // Using None as a default, would be determined by diagnosis code
                                current_date,      // First SCD date
                                DiseaseSeverity::Moderate, // Default severity
                                DiseaseOrigin::Acquired, // Default origin
                            );

                            // TODO: Update the child in the collection
                            // This would require more complex update logic

                            scd_count += 1;
                        }
                    }
                }
            }
        }

        log::info!("Linked {} children with SCD diagnoses", scd_count);

        Ok(())
    }

    /// Link income data to parents
    pub fn link_income_to_parents(&mut self) -> Result<()> {
        // Count of income records linked
        let income_count = 0;

        // TODO: Implement income linking to parents
        // This requires accessing and updating the Parent objects in families

        log::info!("Linked income data to {} parents", income_count);

        Ok(())
    }

    /// Perform sibling identification and linkage
    pub fn identify_siblings(&mut self) -> Result<()> {
        // Count of sibling relationships identified
        let mut sibling_count = 0;

        // Create a mapping of family_id to children
        let mut family_children: HashMap<String, Vec<Arc<Child>>> = HashMap::new();

        // Identify families with children
        let family_ids = self
            .collection
            .get_snapshots_at(&chrono::Utc::now().naive_utc().date())
            .into_iter()
            .map(|snapshot| snapshot.family_id)
            .collect::<Vec<_>>();

        for family_id in family_ids {
            if let Some(family) = self.collection.get_family(&family_id) {
                if family.children.len() > 1 {
                    // This family has multiple children (siblings)
                    family_children.insert(family_id, family.children.clone());
                    sibling_count += family.children.len();
                }
            }
        }

        log::info!(
            "Identified {} children in sibling relationships across {} families",
            sibling_count,
            family_children.len()
        );

        // TODO: Use the sibling relationships for further analysis
        // For example, determine if having a sibling with SCD affects outcomes

        Ok(())
    }
}

impl Default for RegistryIntegration {
    fn default() -> Self {
        Self::new()
    }
}
