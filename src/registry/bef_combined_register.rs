//! BEF registry combined loader with relationship extraction
//!
//! This module provides a helper for working with BEF data to extract families and relationships

use crate::error::Result;
use crate::models::{Family, Individual};
use crate::common::traits::BefRegistry;
use arrow::record_batch::RecordBatch;
use chrono::Datelike;

/// Helper for extracting relationships from BEF data
pub struct BefCombinedRegister;

impl BefCombinedRegister {
    /// Process a batch of BEF data to extract individuals and families
    pub fn process_batch(batch: &RecordBatch) -> Result<(Vec<Individual>, Vec<Family>)> {
        // This is a simplified implementation that delegates to the BEF model conversion
        let individuals = Individual::from_bef_batch(batch)?;

        // In a complete implementation, we would also extract and construct families
        // For now, return an empty vector
        let families = Vec::new();

        Ok((individuals, families))
    }

    /// Extract family relationships from a set of individuals
    #[must_use]
    pub fn extract_relationships(
        individuals: &[Individual],
    ) -> Vec<(String, (Option<String>, Option<String>, Vec<String>))> {
        let mut relationships = Vec::new();

        // Group individuals by family ID
        let mut family_members: std::collections::HashMap<String, Vec<&Individual>> =
            std::collections::HashMap::new();

        for individual in individuals {
            if let Some(family_id) = &individual.family_id {
                family_members
                    .entry(family_id.clone())
                    .or_default()
                    .push(individual);
            }
        }

        // Extract relationships for each family
        for (family_id, members) in family_members {
            let mut mother_pnr = None;
            let mut father_pnr = None;
            let mut children_pnrs = Vec::new();

            for member in members {
                // Simplified role detection based on available data
                // In a complete implementation, we would use proper role flags
                // For now, use gender and age to infer roles
                if member.gender == "F".into()
                    && member.birth_date.is_some_and(|bd| bd.year() < 1990)
                {
                    // Assume adult females are mothers
                    mother_pnr = Some(member.pnr.clone());
                } else if member.gender == "M".into()
                    && member.birth_date.is_some_and(|bd| bd.year() < 1990)
                {
                    // Assume adult males are fathers
                    father_pnr = Some(member.pnr.clone());
                } else if member.birth_date.is_some_and(|bd| bd.year() >= 1990) {
                    // Assume anyone born after 1990 is a child
                    children_pnrs.push(member.pnr.clone());
                }
            }

            // Add relationship if we have at least one parent and one child
            if (mother_pnr.is_some() || father_pnr.is_some()) && !children_pnrs.is_empty() {
                relationships.push((family_id, (mother_pnr, father_pnr, children_pnrs)));
            }
        }

        relationships
    }
}
