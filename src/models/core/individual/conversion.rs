//! Arrow schema conversion and serialization
//!
//! This module contains the Arrow schema conversion implementations
//! for serializing and deserializing Individual models.

use crate::error::Result;
use crate::models::core::individual::Individual;
use crate::models::core::traits::ArrowSchema;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

// Implement ArrowSchema trait
impl ArrowSchema for Individual {
    /// Get the Arrow schema for Individual records
    fn schema() -> Schema {
        Schema::new(vec![
            // Core demographic fields
            Field::new("pnr", DataType::Utf8, false),
            Field::new("gender", DataType::Int32, false),
            Field::new("birth_date", DataType::Date32, true),
            Field::new("death_date", DataType::Date32, true),
            Field::new("origin", DataType::Int32, false),
            Field::new("education_level", DataType::Int32, false),
            Field::new("municipality_code", DataType::Utf8, true),
            Field::new("is_rural", DataType::Boolean, false),
            Field::new("mother_pnr", DataType::Utf8, true),
            Field::new("father_pnr", DataType::Utf8, true),
            Field::new("family_id", DataType::Utf8, true),
            Field::new("emigration_date", DataType::Date32, true),
            Field::new("immigration_date", DataType::Date32, true),
            // Employment and socioeconomic status fields
            Field::new("socioeconomic_status", DataType::Int32, false),
            Field::new("occupation_code", DataType::Utf8, true),
            Field::new("industry_code", DataType::Utf8, true),
            Field::new("workplace_id", DataType::Utf8, true),
            Field::new("employment_start_date", DataType::Date32, true),
            Field::new("working_hours", DataType::Float64, true),
            // Education details fields
            Field::new("education_field", DataType::Int32, false),
            Field::new("education_completion_date", DataType::Date32, true),
            Field::new("education_institution", DataType::Utf8, true),
            Field::new("education_program_code", DataType::Utf8, true),
            // Income information fields
            Field::new("annual_income", DataType::Float64, true),
            Field::new("disposable_income", DataType::Float64, true),
            Field::new("employment_income", DataType::Float64, true),
            Field::new("self_employment_income", DataType::Float64, true),
            Field::new("capital_income", DataType::Float64, true),
            Field::new("transfer_income", DataType::Float64, true),
            Field::new("income_year", DataType::Int32, true),
            // Healthcare usage fields
            Field::new("hospital_admissions_count", DataType::Int32, true),
            Field::new("emergency_visits_count", DataType::Int32, true),
            Field::new("outpatient_visits_count", DataType::Int32, true),
            Field::new("gp_visits_count", DataType::Int32, true),
            Field::new("last_hospital_admission_date", DataType::Date32, true),
            Field::new("hospitalization_days", DataType::Int32, true),
            // Additional demographic information fields
            Field::new("marital_status", DataType::Int32, false),
            Field::new("citizenship_status", DataType::Int32, false),
            Field::new("housing_type", DataType::Int32, false),
            Field::new("household_size", DataType::Int32, true),
            Field::new("household_type", DataType::Utf8, true),
        ])
    }

    /// Convert a `RecordBatch` to a vector of Individual objects
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        match serde_arrow::from_record_batch(batch) {
            Ok(individuals) => Ok(individuals),
            Err(e) => Err(anyhow::anyhow!("Failed to deserialize: {}", e)),
        }
    }

    /// Convert a vector of Individual objects to a `RecordBatch`
    fn to_record_batch(individuals: &[Self]) -> Result<RecordBatch> {
        // Use the predefined schema to ensure consistent date handling
        let schema = Self::schema();

        // Convert schema fields to a Vec<FieldRef>
        let fields: Vec<arrow::datatypes::FieldRef> = schema
            .fields()
            .iter()
            .map(std::sync::Arc::clone)
            .collect();

        // Convert to record batch using the defined schema fields
        serde_arrow::to_record_batch(&fields, &individuals)
            .map_err(|e| anyhow::anyhow!("Serialization error: {}", e))
    }
}
