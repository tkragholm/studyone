//! Preparation utilities for case-control matching
//! 
//! This module provides utilities for preparing data for case-control matching,
//! including converting between population objects and Arrow RecordBatches.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{
    BooleanBuilder, Date32Builder, Float64Builder, Int32Builder, StringBuilder, UInt8Builder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{Datelike, NaiveDate};

use crate::error::Result;
use crate::algorithm::population::Population;
use crate::models::Individual;

/// Prepare case and control groups from a population and list of individuals with a condition
/// 
/// This function takes a population and a list of PNRs for individuals with a specific
/// condition (such as SCD) and converts them into Arrow RecordBatches suitable for
/// use with the matching algorithm.
pub fn prepare_case_control_groups(
    population: &Population,
    individuals_with_condition: &[String],
) -> Result<(
    arrow::record_batch::RecordBatch,
    arrow::record_batch::RecordBatch,
)> {
    // Convert individuals to record batches
    let condition_set: HashSet<String> = individuals_with_condition.iter().cloned().collect();

    // Split individuals into cases and controls
    let mut cases = Vec::new();
    let mut controls = Vec::new();

    for individual in population.collection.get_individuals() {
        if condition_set.contains(&individual.pnr) {
            cases.push(individual.clone());
        } else {
            controls.push(individual.clone());
        }
    }

    // Define schema for record batches
    let schema = Arc::new(Schema::new(vec![
        Field::new("pnr", DataType::Utf8, false),
        Field::new("birthdate", DataType::Date32, true),
        Field::new("gender", DataType::UInt8, true),
        Field::new("age", DataType::Int32, true),
        Field::new("is_rural", DataType::Boolean, true),
        Field::new("education_level", DataType::UInt8, true),
        Field::new("municipality_code", DataType::Utf8, true),
        Field::new("income", DataType::Float64, true),
    ]));

    // Helper function to convert individuals to a record batch
    let convert_to_batch =
        |individuals: Vec<Arc<Individual>>| -> Result<RecordBatch> {
            // Create array builders
            let mut pnr_builder = StringBuilder::new();
            let mut birthdate_builder = Date32Builder::new();
            let mut gender_builder = UInt8Builder::new();
            let mut age_builder = Int32Builder::new();
            let mut is_rural_builder = BooleanBuilder::new();
            let mut education_builder = UInt8Builder::new();
            let mut municipality_builder = StringBuilder::new();
            let mut income_builder = Float64Builder::new();

            // Add data
            for individual in individuals {
                // PNR (required)
                pnr_builder.append_value(&individual.pnr);

                // Birthdate
                if let Some(date) = individual.birth_date {
                    let days_since_epoch = date
                        .signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                        .num_days() as i32;
                    birthdate_builder.append_value(days_since_epoch);
                } else {
                    birthdate_builder.append_null();
                }

                // Gender
                gender_builder.append_value(individual.gender as u8);

                // Age (calculated from birthdate and the study index date)
                if let Some(birthdate) = individual.birth_date {
                    let index_date = NaiveDate::from_ymd_opt(2018, 1, 1).unwrap(); // Same as population config
                    let age = index_date.year() - birthdate.year();
                    if index_date.ordinal() < birthdate.ordinal() {
                        age_builder.append_value(age - 1);
                    } else {
                        age_builder.append_value(age);
                    }
                } else {
                    age_builder.append_null();
                }

                // Rural status
                is_rural_builder.append_value(individual.is_rural);

                // Education level
                education_builder.append_value(individual.education_level as u8);

                // Municipality code
                if let Some(code) = &individual.municipality_code {
                    municipality_builder.append_value(code);
                } else {
                    municipality_builder.append_null();
                }

                // Income (optional)
                income_builder.append_null(); // We don't have income in our test data
            }

            // Create arrays
            let arrays = vec![
                Arc::new(pnr_builder.finish()) as _,
                Arc::new(birthdate_builder.finish()) as _,
                Arc::new(gender_builder.finish()) as _,
                Arc::new(age_builder.finish()) as _,
                Arc::new(is_rural_builder.finish()) as _,
                Arc::new(education_builder.finish()) as _,
                Arc::new(municipality_builder.finish()) as _,
                Arc::new(income_builder.finish()) as _,
            ];

            // Create record batch
            RecordBatch::try_new(schema.clone(), arrays)
                .map_err(|e| anyhow::anyhow!("Failed to create record batch: {}", e).into())
        };

    // Convert cases and controls to record batches
    let case_batch = convert_to_batch(cases)?;
    let control_batch = convert_to_batch(controls)?;

    Ok((case_batch, control_batch))
}