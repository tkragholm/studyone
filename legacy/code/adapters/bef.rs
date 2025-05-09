//! BEF Registry Adapters
//!
//! This module provides adapters for converting BEF registry data
//! to Individual and Family domain models using the unified adapter interface.

use crate::error::Result;
use crate::common::traits::{RegistryAdapter, StatefulAdapter, BatchProcessor, ModelLookup};
use crate::models::{Individual, Family};
use crate::registry::{BefRegister, ModelConversion};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

/// Adapter for converting BEF registry data to Individual models
#[derive(Debug)]
pub struct BefIndividualAdapter;

impl RegistryAdapter<Individual> for BefIndividualAdapter {
    /// Convert a BEF registry batch to Individual models
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Individual>> {
        let registry = BefRegister::new();
        registry.to_models(batch)
    }
    
    /// Apply transformations to Individual models
    fn transform(models: &mut [Individual]) -> Result<()> {
        let registry = BefRegister::new();
        registry.transform_models(models)
    }
}

/// Adapter for converting BEF registry data to Family models
#[derive(Debug)]
pub struct BefFamilyAdapter;

impl RegistryAdapter<Family> for BefFamilyAdapter {
    /// Convert a BEF registry batch to Family models
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Family>> {
        let registry = BefRegister::new();
        registry.to_models(batch)
    }
    
    /// Apply transformations to Family models
    fn transform(models: &mut [Family]) -> Result<()> {
        let registry = BefRegister::new();
        registry.transform_models(models)
    }
}

/// Adapter for providing lookup functions for Individual models
impl ModelLookup<Individual, String> for Individual {
    /// Create a lookup map from PNR to Individual
    fn create_lookup(individuals: &[Individual]) -> HashMap<String, Arc<Individual>> {
        let mut lookup = HashMap::with_capacity(individuals.len());
        for individual in individuals {
            lookup.insert(individual.pnr.clone(), Arc::new(individual.clone()));
        }
        lookup
    }
    
    // The create_lookup_with method is now provided by the helper function in adapters/mod.rs
}

/// Combined adapter that processes BEF data to extract both Individuals and Families
#[derive(Debug)]
pub struct BefCombinedAdapter {
    registry: BefRegister,
}

impl Default for BefCombinedAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl BefCombinedAdapter {
    /// Create a new combined adapter
    #[must_use] pub fn new() -> Self {
        Self {
            registry: BefRegister::new(),
        }
    }
    
    /// Process a batch to extract both Individuals and Families
    pub fn process_combined_batch(&self, batch: &RecordBatch) -> Result<(Vec<Individual>, Vec<Family>)> {
        // Extract individuals and families from the batch
        let individuals = ModelConversion::<Individual>::to_models(&self.registry, batch)?;
        let families = ModelConversion::<Family>::to_models(&self.registry, batch)?;
        
        // Apply any needed transformations
        let mut individuals_mut = individuals.clone();
        self.registry.transform_models(&mut individuals_mut)?;
        
        let mut families_mut = families.clone();
        self.registry.transform_models(&mut families_mut)?;
        
        Ok((individuals_mut, families_mut))
    }
    
    /// Extract family relationships from a collection of individuals
    #[must_use] pub fn extract_relationships(&self, individuals: &[Individual]) -> HashMap<String, (Option<String>, Option<String>, Vec<String>)> {
        let mut relationships: HashMap<String, (Option<String>, Option<String>, Vec<String>)> = HashMap::new();
        
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
                // Check if this individual is a parent
                let is_parent = members.iter().any(|m| {
                    (m.mother_pnr.as_ref() == Some(&member.pnr))
                        || (m.father_pnr.as_ref() == Some(&member.pnr))
                });
                
                if is_parent {
                    // This is a parent
                    match member.gender {
                        crate::models::types::Gender::Female => {
                            mother_pnr = Some(member.pnr.clone());
                        }
                        crate::models::types::Gender::Male => {
                            father_pnr = Some(member.pnr.clone());
                        }
                        crate::models::types::Gender::Unknown => {} // Skip unknown gender
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

impl StatefulAdapter<Individual> for BefCombinedAdapter {
    fn convert_batch(&self, batch: &RecordBatch) -> Result<Vec<Individual>> {
        self.registry.to_models(batch)
    }
    
    fn transform_models(&self, models: &mut [Individual]) -> Result<()> {
        self.registry.transform_models(models)
    }
}

impl StatefulAdapter<Family> for BefCombinedAdapter {
    fn convert_batch(&self, batch: &RecordBatch) -> Result<Vec<Family>> {
        self.registry.to_models(batch)
    }
    
    fn transform_models(&self, models: &mut [Family]) -> Result<()> {
        self.registry.transform_models(models)
    }
}

impl BatchProcessor for BefCombinedAdapter {
    // This doesn't do anything because the process_batch method is
    // defined with a concrete type signature in process_combined_batch
}