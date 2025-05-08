//! BEF registry model conversions
//!
//! This module implements registry-specific conversions for BEF registry data.
//! It provides trait implementations to convert from BEF registry format to domain models.
//! It also provides compatibility with the old ModelConversion interface.

use crate::RecordBatch;
use crate::common::traits::BefRegistry;
use crate::error::Result;
use crate::models::Family;
use crate::models::Individual;
use crate::models::traits::HealthStatus;
use crate::models::types::{FamilyType, Gender};
use crate::registry::{BefRegister, ModelConversion};
use arrow::array::Array;
use std::collections::HashMap;

// BefRegistry trait is implemented in models/individual.rs
// This file used to contain a duplicate implementation
// which has been removed to avoid conflicting implementations

// Maintain compatibility with the old ModelConversion interface
impl ModelConversion<Individual> for BefRegister {
    /// Convert BEF registry data to Individual domain models
    fn to_models(&self, batch: &RecordBatch) -> Result<Vec<Individual>> {
        // Use the trait implementation from Individual (in models/individual.rs)
        use crate::common::traits::BefRegistry;
        Individual::from_bef_batch(batch)
    }

    /// Convert Individual domain models back to BEF registry data
    fn from_models(&self, _models: &[Individual]) -> Result<RecordBatch> {
        unimplemented!("Conversion from Individual models to BEF registry data not yet implemented")
    }

    /// Apply additional transformations to Individual models
    fn transform_models(&self, _models: &mut [Individual]) -> Result<()> {
        Ok(())
    }
}

// Maintain compatibility with ModelConversion for Family
impl ModelConversion<Family> for BefRegister {
    /// Convert BEF registry data to Family domain models
    fn to_models(&self, batch: &RecordBatch) -> Result<Vec<Family>> {
        // First get individual data
        let individuals = Individual::from_bef_batch(batch)?;

        // Generate family models
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
                            Gender::Female => mothers.push(member),
                            Gender::Male => fathers.push(member),
                            Gender::Unknown => {} // Skip individuals with unknown gender
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
    fn from_models(&self, _models: &[Family]) -> Result<RecordBatch> {
        unimplemented!("Conversion from Family models to BEF registry data not yet implemented")
    }

    /// Apply additional transformations to Family models
    fn transform_models(&self, _models: &mut [Family]) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringBuilder;
    use arrow::datatypes::{Field, Schema};

    #[test]
    fn test_individual_from_bef_record() -> Result<()> {
        // Create a test batch with BEF data
        let schema = Schema::new(vec![
            Field::new("PNR", DataType::Utf8, false),
            Field::new("KOEN", DataType::Utf8, true),
        ]);

        let mut batch_builder = RecordBatchBuilder::new_with_capacity(schema, 1);

        // Add PNR data
        let mut pnr_builder = StringBuilder::new_with_capacity(1, 12);
        pnr_builder.append_value("1234567890")?;
        batch_builder
            .column_builder::<StringBuilder>(0)
            .unwrap()
            .append_builder(&pnr_builder)?;

        // Add gender data
        let mut gender_builder = StringBuilder::new_with_capacity(1, 1);
        gender_builder.append_value("M")?;
        batch_builder
            .column_builder::<StringBuilder>(1)
            .unwrap()
            .append_builder(&gender_builder)?;

        let batch = batch_builder.build()?;

        // Test conversion
        let individual = Individual::from_bef_record(&batch, 0)?;

        assert!(individual.is_some());
        let individual = individual.unwrap();
        assert_eq!(individual.pnr, "1234567890");
        assert!(matches!(individual.gender, Gender::Male));

        Ok(())
    }
}
