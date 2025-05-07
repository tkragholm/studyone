//! BEF registry model conversion implementations
//!
//! This module implements bidirectional conversion between BEF registry data
//! and domain models (Individual, Family).

use crate::registry::{BefRegister, ModelConversion};
use crate::error::Result;
use crate::models::{Individual, Family};
use crate::RecordBatch;

// Implement conversion from BEF to Individual models
impl ModelConversion<Individual> for BefRegister {
    /// Convert BEF registry data to Individual domain models
    ///
    /// This method uses the schema-aware constructors on the Individual model
    /// to handle conversion with proper error handling and type adaptation.
    ///
    /// # Arguments
    ///
    /// * `batch` - The record batch with BEF schema
    ///
    /// # Returns
    ///
    /// * `Result<Vec<Individual>>` - The created Individuals or an error
    fn to_models(&self, batch: &RecordBatch) -> Result<Vec<Individual>> {
        // Use the schema-aware constructor on Individual
        Individual::from_bef_batch(batch)
    }
    
    /// Convert Individual domain models back to BEF registry data
    ///
    /// This method creates a record batch with BEF schema from Individual models.
    /// 
    /// # Arguments
    ///
    /// * `models` - The Individual models to convert
    ///
    /// # Returns
    ///
    /// * `Result<RecordBatch>` - The created record batch or an error
    fn from_models(&self, _models: &[Individual]) -> Result<RecordBatch> {
        // This would be implemented with arrow array builders for each field
        // matching the BEF schema
        unimplemented!("Conversion from Individual models to BEF registry data not yet implemented")
    }
    
    /// Apply additional transformations to Individual models
    ///
    /// This method can apply additional BEF-specific transformations that aren't
    /// handled in the basic conversion.
    ///
    /// # Arguments
    ///
    /// * `models` - The Individual models to transform
    ///
    /// # Returns
    ///
    /// * `Result<()>` - Success or error
    fn transform_models(&self, _models: &mut [Individual]) -> Result<()> {
        // No additional transformations needed for individuals from BEF
        Ok(())
    }
}

// Implement conversion from BEF to Family models
impl ModelConversion<Family> for BefRegister {
    /// Convert BEF registry data to Family domain models
    ///
    /// This method first converts to Individual models and then derives 
    /// Family models from them.
    ///
    /// # Arguments
    ///
    /// * `batch` - The record batch with BEF schema
    ///
    /// # Returns
    ///
    /// * `Result<Vec<Family>>` - The created Family models or an error
    fn to_models(&self, batch: &RecordBatch) -> Result<Vec<Family>> {
        // First get individual data
        let individuals = Individual::from_bef_batch(batch)?;
        
        // Generate family models - this is the same logic from bef_adapter.rs
        use crate::models::family::{Family, FamilyType};
        use std::collections::HashMap;
        
        // Group individuals by family_id
        let mut families_map: HashMap<String, Vec<&Individual>> = HashMap::new();
        for individual in &individuals {
            if let Some(family_id) = &individual.family_id {
                families_map
                    .entry(family_id.clone())
                    .or_default()
                    .push(individual);
            }
        }
        
        // Create Family objects from grouped individuals
        let mut families = Vec::new();
        let current_date = chrono::Utc::now().naive_utc().date();
        
        for (family_id, members) in families_map {
            // Find parents and children in the family
            let mut mothers = Vec::new();
            let mut fathers = Vec::new();
            let mut children = Vec::new();
            
            for member in &members {
                // Simple heuristic: adults (18+) are potential parents, others are children
                if let Some(age) = member.age_at(&current_date) {
                    if age >= 18 {
                        match member.gender {
                            crate::models::individual::Gender::Female => mothers.push(member),
                            crate::models::individual::Gender::Male => fathers.push(member),
                            crate::models::individual::Gender::Unknown => {} // Skip individuals with unknown gender
                        }
                    } else {
                        children.push(member);
                    }
                }
            }
            
            // Determine family type
            let family_type = match (mothers.len(), fathers.len()) {
                (1.., 1..) => FamilyType::TwoParent,
                (1.., 0) => FamilyType::SingleMother,
                (0, 1..) => FamilyType::SingleFather,
                (0, 0) => FamilyType::NoParent,
            };
            
            // Create a new family
            // Since we don't have specific valid_from dates, we'll use a default
            let default_valid_from = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
            let family = Family::new(family_id, family_type, default_valid_from);
            
            families.push(family);
        }
        
        Ok(families)
    }
    
    /// Convert Family domain models back to BEF registry data
    ///
    /// # Arguments
    ///
    /// * `models` - The Family models to convert
    ///
    /// # Returns
    ///
    /// * `Result<RecordBatch>` - The created record batch or an error
    fn from_models(&self, _models: &[Family]) -> Result<RecordBatch> {
        // This would be implemented with arrow array builders for each field
        // matching the BEF schema
        unimplemented!("Conversion from Family models to BEF registry data not yet implemented")
    }
    
    /// Apply additional transformations to Family models
    ///
    /// # Arguments
    ///
    /// * `models` - The Family models to transform
    ///
    /// # Returns
    ///
    /// * `Result<()>` - Success or error
    fn transform_models(&self, _models: &mut [Family]) -> Result<()> {
        // No additional transformations needed for families from BEF
        Ok(())
    }
}