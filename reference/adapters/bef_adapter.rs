//! Refactored BEF Registry to Individual/Family Adapter
//!
//! This module contains the adapter that maps BEF registry data to Individual and Family domain models,
//! but using the new ModelConversion approach for a tighter integration between registries and models.

use super::RegistryAdapter;
use crate::error::Result;
use crate::models::family::Family;
use crate::models::individual::Individual;
use crate::registry::{BefRegister, ModelConversion};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;

/// Adapter for converting BEF registry data to Individual models
///
/// This refactored version uses the new ModelConversion trait
/// which is directly implemented on BefRegister.
pub struct BefIndividualAdapter;

impl RegistryAdapter<Individual> for BefIndividualAdapter {
    /// Convert a BEF `RecordBatch` to a vector of Individual objects
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Individual>> {
        // Create a registry instance
        let bef_registry = BefRegister::new();

        // Use the ModelConversion trait to convert the batch
        bef_registry.to_models(batch)
    }

    /// Apply additional transformations to the Individual models
    fn transform(models: &mut [Individual]) -> Result<()> {
        let bef_registry = BefRegister::new();
        bef_registry.transform_models(models)
    }
}

/// Adapter for converting BEF registry data to Family models
///
/// This refactored version uses the new ModelConversion trait
/// which is directly implemented on BefRegister.
pub struct BefFamilyAdapter;

impl RegistryAdapter<Family> for BefFamilyAdapter {
    /// Convert a BEF `RecordBatch` to a vector of Family objects
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Family>> {
        // Create a registry instance
        let bef_registry = BefRegister::new();

        // Use the ModelConversion trait to convert the batch
        bef_registry.to_models(batch)
    }

    /// Apply additional transformations to the Family models
    fn transform(models: &mut [Family]) -> Result<()> {
        let bef_registry = BefRegister::new();
        bef_registry.transform_models(models)
    }
}

/// Helper function to create a lookup of Individual objects by PNR
#[must_use]
pub fn create_individual_lookup(individuals: &[Individual]) -> HashMap<String, Arc<Individual>> {
    let mut lookup = HashMap::new();
    for individual in individuals {
        lookup.insert(individual.pnr.clone(), Arc::new(individual.clone()));
    }
    lookup
}

/// Combined adapter that processes BEF data and returns both Individuals and Families
pub struct BefCombinedAdapter;

impl BefCombinedAdapter {
    /// Process a BEF `RecordBatch` and return both Individuals and Families
    pub fn process_batch(batch: &RecordBatch) -> Result<(Vec<Individual>, Vec<Family>)> {
        // Create a registry instance
        let bef_registry = BefRegister::new();

        // Use the ModelConversion trait to convert the batch
        let individuals = ModelConversion::<Individual>::to_models(&bef_registry, batch)?;
        let families = ModelConversion::<Family>::to_models(&bef_registry, batch)?;

        Ok((individuals, families))
    }

    /// Extract unique family relationships from BEF data
    #[must_use]
    pub fn extract_relationships(
        individuals: &[Individual],
    ) -> HashMap<String, (Option<String>, Option<String>, Vec<String>)> {
        let mut relationships: HashMap<String, (Option<String>, Option<String>, Vec<String>)> =
            HashMap::new();

        // Group individuals by family ID
        let mut family_members: HashMap<String, Vec<&Individual>> = HashMap::new();
        for individual in individuals {
            if let Some(family_id) = &individual.family_id {
                family_members
                    .entry(family_id.clone())
                    .or_default()
                    .push(individual);
            }
        }

        // Process each family
        for (family_id, members) in family_members {
            let mut children = Vec::new();
            let mut mother_pnr = None;
            let mut father_pnr = None;

            for member in &members {
                // Check if this individual is a parent of any other individual in the family
                let is_parent = members.iter().any(|m| {
                    (m.mother_pnr.as_ref() == Some(&member.pnr))
                        || (m.father_pnr.as_ref() == Some(&member.pnr))
                });

                if is_parent {
                    // This is a parent
                    match member.gender {
                        crate::models::individual::Gender::Female => {
                            mother_pnr = Some(member.pnr.clone())
                        }
                        crate::models::individual::Gender::Male => {
                            father_pnr = Some(member.pnr.clone())
                        }
                        crate::models::individual::Gender::Unknown => {} // Skip individuals with unknown gender
                    }
                } else {
                    // This is likely a child
                    children.push(member.pnr.clone());
                }
            }

            relationships.insert(family_id, (mother_pnr, father_pnr, children));
        }

        relationships
    }
}
