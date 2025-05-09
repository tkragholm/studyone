//! UDDF registry trait implementations for Individual
//!
//! This module contains the implementation of `UddfRegistry` for the Individual model.

use crate::RecordBatch;
use crate::common::traits::UddfRegistry;
use crate::error::Result;
use crate::models::core::Individual;
use crate::models::core::types::{EducationField, EducationLevel};
use crate::utils::field_extractors::{extract_date32, extract_int32, extract_string};

impl UddfRegistry for Individual {
    fn from_uddf_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>> {
        // Use the serde_arrow-based deserializer for the row
        crate::registry::uddf::deserializer::deserialize_row(batch, row)
    }

    fn from_uddf_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        // Use the serde_arrow-based deserializer for the batch
        crate::registry::uddf::deserializer::deserialize_batch(batch)
    }

    fn enhance_with_education_data(&mut self, batch: &RecordBatch, row: usize) -> Result<bool> {
        // Extract education level from UDD_H
        if let Ok(Some(level_code)) = extract_int32(batch, row, "UDD_H", false) {
            self.education_level = match level_code {
                10 => EducationLevel::Low,
                20 | 30 => EducationLevel::Medium,
                40 | 50 | 60 => EducationLevel::High,
                _ => EducationLevel::Unknown,
            };
        }

        // Extract education field from AUDD
        if let Ok(Some(field_code)) = extract_string(batch, row, "AUDD", false) {
            if !field_code.is_empty() {
                self.education_field = match &field_code[0..1] {
                    "0" => EducationField::General,
                    "1" => EducationField::Education,
                    "2" => EducationField::HumanitiesArts,
                    "3" => EducationField::SocialScienceBusinessLaw,
                    "4" => EducationField::ScienceMathematicsComputing,
                    "5" => EducationField::EngineeringManufacturingConstruction,
                    "6" => EducationField::AgricultureVeterinary,
                    "7" => EducationField::HealthWelfare,
                    "8" => EducationField::Services,
                    _ => EducationField::Unknown,
                };

                self.education_program_code = Some(field_code);
            }
        }

        // Extract education institution
        if let Ok(Some(institution)) = extract_string(batch, row, "UDD_INST", false) {
            self.education_institution = Some(institution);
        }

        // Extract education completion date
        if let Ok(Some(completion_date)) = extract_date32(batch, row, "AFSLUTNINGSDATO", false) {
            self.education_completion_date = Some(completion_date);
        }

        Ok(true)
    }
}
