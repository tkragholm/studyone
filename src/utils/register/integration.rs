//! Registry Integration Utilities
//!
//! This module provides shared utilities for working with registry data across the application.
//! It includes functions for column extraction, date conversion, and standardized registry operations.

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Context;
use arrow::array::{Array, Date32Array, Int32Array, StringArray};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;

use crate::error::{ParquetReaderError, Result};
use crate::models::core::Individual;
use crate::utils::arrow::conversion;

/// Trait defining a registry interface for consistent data integration
pub trait Registry: Send + Sync {
    /// Get the registry name
    fn name(&self) -> &'static str;

    /// Get the registry schema
    fn schema(&self) -> SchemaRef;

    /// Load data from the registry with optional filtering
    fn load(&self, path: &Path, filter: Option<&HashSet<String>>) -> Result<Vec<RecordBatch>>;

    /// Load data asynchronously
    fn load_async<'a>(
        &'a self,
        path: &'a Path,
        filter: Option<&'a HashSet<String>>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<RecordBatch>>> + Send + 'a>>;

    /// Check if this registry can be integrated with another registry
    fn can_integrate_with(&self, other: &dyn Registry) -> bool;
}

/// Configuration for registry date range filtering
#[derive(Debug, Clone)]
pub struct DateRangeConfig {
    /// Start date for filtering (inclusive)
    pub start_date: Option<NaiveDate>,
    /// End date for filtering (inclusive)
    pub end_date: Option<NaiveDate>,
    /// Name of the column containing the date to filter on
    pub date_column: String,
}

impl DateRangeConfig {
    /// Create a new date range configuration
    #[must_use]
    pub fn new(
        start_date: Option<NaiveDate>,
        end_date: Option<NaiveDate>,
        date_column: &str,
    ) -> Self {
        Self {
            start_date,
            end_date,
            date_column: date_column.to_string(),
        }
    }

    /// Check if a date is within the configured range
    #[must_use]
    pub fn is_in_range(&self, date: &NaiveDate) -> bool {
        // Check start date constraint
        if let Some(start) = self.start_date {
            if date < &start {
                return false;
            }
        }

        // Check end date constraint
        if let Some(end) = self.end_date {
            if date > &end {
                return false;
            }
        }

        true
    }
}

/// Convert Arrow Date32 value to `NaiveDate`
#[must_use]
pub fn arrow_date_to_naive_date(days_since_epoch: i32) -> NaiveDate {
    conversion::arrow_date_to_naive_date(days_since_epoch)
}

/// Configuration for column mappings between registries
#[derive(Debug, Clone, Default)]
pub struct ColumnMappingConfig {
    /// Mapping from source columns to target columns
    mappings: HashMap<String, String>,
}

impl ColumnMappingConfig {
    /// Create a new empty column mapping
    #[must_use]
    pub fn new() -> Self {
        Self {
            mappings: HashMap::new(),
        }
    }

    /// Add a mapping from source column to target column
    pub fn add_mapping(&mut self, source: &str, target: &str) -> &mut Self {
        self.mappings.insert(source.to_string(), target.to_string());
        self
    }

    /// Get the target column name for a source column
    #[must_use]
    pub fn get_target(&self, source: &str) -> Option<&String> {
        self.mappings.get(source)
    }

    /// Create a column mapping for LPR2 registry
    #[must_use]
    pub fn lpr2_default() -> Self {
        let mut config = Self::new();
        config
            .add_mapping("PNR", "patient_id")
            .add_mapping("C_ADIAG", "primary_diagnosis")
            .add_mapping("C_DIAGTYPE", "diagnosis_type")
            .add_mapping("D_INDDTO", "admission_date")
            .add_mapping("D_UDDTO", "discharge_date");
        config
    }

    /// Create a column mapping for LPR3 registry
    #[must_use]
    pub fn lpr3_default() -> Self {
        let mut config = Self::new();
        config
            .add_mapping("cpr", "patient_id")
            .add_mapping("diagnosekode", "primary_diagnosis")
            .add_mapping("diagnose_type", "diagnosis_type")
            .add_mapping("starttidspunkt", "admission_date")
            .add_mapping("sluttidspunkt", "discharge_date");
        config
    }
}

/// Extract a string column from a record batch
pub fn get_string_column<'a>(batch: &'a RecordBatch, column_name: &str) -> Result<&'a StringArray> {
    batch
        .column_by_name(column_name)
        .ok_or_else(|| ParquetReaderError::column_not_found(column_name))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            ParquetReaderError::InvalidDataType {
                column: column_name.to_string(),
                expected: "StringArray".to_string(),
            }
            .into()
        })
}

/// Extract a date column from a record batch
pub fn get_date_column<'a>(batch: &'a RecordBatch, column_name: &str) -> Result<&'a Date32Array> {
    batch
        .column_by_name(column_name)
        .ok_or_else(|| ParquetReaderError::column_not_found(column_name))?
        .as_any()
        .downcast_ref::<Date32Array>()
        .ok_or_else(|| {
            ParquetReaderError::InvalidDataType {
                column: column_name.to_string(),
                expected: "Date32Array".to_string(),
            }
            .into()
        })
}

/// Extract an integer column from a record batch
pub fn get_int_column<'a>(batch: &'a RecordBatch, column_name: &str) -> Result<&'a Int32Array> {
    batch
        .column_by_name(column_name)
        .ok_or_else(|| ParquetReaderError::column_not_found(column_name))?
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| {
            ParquetReaderError::InvalidDataType {
                column: column_name.to_string(),
                expected: "Int32Array".to_string(),
            }
            .into()
        })
}

/// Filter a record batch by date range
pub fn filter_batch_by_date_range(
    batch: &RecordBatch,
    config: &DateRangeConfig,
) -> Result<RecordBatch> {
    // If no date constraints, return the original batch
    if config.start_date.is_none() && config.end_date.is_none() {
        return Ok(batch.clone());
    }

    // Get the date column
    let date_array = get_date_column(batch, &config.date_column)?;

    // Create a mask for the filter
    let mut mask = Vec::with_capacity(batch.num_rows());

    for i in 0..batch.num_rows() {
        if date_array.is_null(i) {
            mask.push(false);
            continue;
        }

        let date = arrow_date_to_naive_date(date_array.value(i));
        mask.push(config.is_in_range(&date));
    }

    // Apply the mask
    let bool_array = arrow::array::BooleanArray::from(mask);
    crate::filter::core::filter_record_batch(batch, &bool_array).with_context(|| {
        format!(
            "Failed to filter batch by date range on column {}",
            config.date_column
        )
    })
}

/// Process batches with a mapping function and collect the results
pub fn process_batches<T, F>(batches: &[RecordBatch], mut process_fn: F) -> Result<Vec<T>>
where
    F: FnMut(&RecordBatch) -> Result<Vec<T>>,
{
    let mut results = Vec::new();

    for batch in batches {
        let batch_results = process_fn(batch)?;
        results.extend(batch_results);
    }

    Ok(results)
}

/// Trait for objects that can be linked to PNR (Danish personal ID)
pub trait PnrLinked {
    /// Get the PNR for this object
    fn pnr(&self) -> &str;
}

/// Group a collection of PNR-linked objects by PNR
pub fn group_by_pnr<T: PnrLinked>(items: Vec<T>) -> HashMap<String, Vec<T>> {
    let mut groups = HashMap::new();

    for item in items {
        groups
            .entry(item.pnr().to_string())
            .or_insert_with(Vec::new)
            .push(item);
    }

    groups
}

/// A trait for registry data transformation to domain models
pub trait RegistryTransformer<T> {
    /// Transform a record batch into domain models
    fn transform(&self, batch: &RecordBatch) -> Result<Vec<T>>;

    /// Transform multiple batches into domain models
    fn transform_batches(&self, batches: &[RecordBatch]) -> Result<Vec<T>> {
        process_batches(batches, |batch| self.transform(batch))
    }
}

/// Base implementation for registry data sources
pub struct BaseRegistry {
    name: &'static str,
    schema: SchemaRef,
}

impl BaseRegistry {
    /// Create a new base registry
    #[must_use]
    pub const fn new(name: &'static str, schema: SchemaRef) -> Self {
        Self { name, schema }
    }
}

impl Registry for BaseRegistry {
    fn name(&self) -> &'static str {
        self.name
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn load(&self, path: &Path, filter: Option<&HashSet<String>>) -> Result<Vec<RecordBatch>> {
        crate::utils::io::parquet::read_parquet::<std::collections::hash_map::RandomState>(
            path,
            Some(&self.schema),
            filter,
            None,
            None,
        )
    }

    fn load_async<'a>(
        &'a self,
        path: &'a Path,
        filter: Option<&'a HashSet<String>>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<RecordBatch>>> + Send + 'a>> {
        Box::pin(async move { self.load(path, filter) })
    }

    fn can_integrate_with(&self, _other: &dyn Registry) -> bool {
        // Base implementation always returns false
        // Specific registries should override this
        false
    }
}

/// A registry integrator that manages loading and transforming data from multiple registries
pub struct RegistryIntegrator {
    registries: HashMap<String, Arc<dyn Registry>>,
}

impl RegistryIntegrator {
    /// Create a new registry integrator
    #[must_use]
    pub fn new() -> Self {
        Self {
            registries: HashMap::new(),
        }
    }

    /// Add a registry to the integrator
    pub fn add_registry(&mut self, registry: Arc<dyn Registry>) -> &mut Self {
        self.registries
            .insert(registry.name().to_string(), registry);
        self
    }

    /// Get a registry by name
    #[must_use]
    pub fn get_registry(&self, name: &str) -> Option<&Arc<dyn Registry>> {
        self.registries.get(name)
    }

    /// Load data from a registry
    pub fn load_registry<'a>(
        &'a self,
        name: &str,
        path: &'a Path,
        filter: Option<&'a HashSet<String>>,
    ) -> Result<Vec<RecordBatch>> {
        let registry = self
            .registries
            .get(name)
            .ok_or_else(|| anyhow::anyhow!("Registry not found: {}", name))?;

        registry.load(path, filter)
    }

    /// Load data from a registry asynchronously
    #[must_use]
    pub fn load_registry_async<'a>(
        &'a self,
        name: &'a str,
        path: &'a Path,
        filter: Option<&'a HashSet<String>>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<RecordBatch>>> + Send + 'a>> {
        Box::pin(async move {
            let registry = self
                .registries
                .get(name)
                .ok_or_else(|| anyhow::anyhow!("Registry not found: {}", name))?;

            registry.load_async(path, filter).await
        })
    }
}

impl Default for RegistryIntegrator {
    fn default() -> Self {
        Self::new()
    }
}

/// Extension trait for `NaiveDate` conversions
pub trait DateConversionExt {
    /// Check if a date is within a range
    fn is_in_range(&self, start: Option<&NaiveDate>, end: Option<&NaiveDate>) -> bool;
}

impl DateConversionExt for NaiveDate {
    fn is_in_range(&self, start: Option<&NaiveDate>, end: Option<&NaiveDate>) -> bool {
        // Check start date constraint
        if let Some(start_date) = start {
            if self < start_date {
                return false;
            }
        }

        // Check end date constraint
        if let Some(end_date) = end {
            if self > end_date {
                return false;
            }
        }

        true
    }
}

/// Collect birth dates from individuals
///
/// Creates a map from PNR to birth date for all individuals
/// which can be used for faster lookups in algorithms that need birth dates.
#[must_use]
pub fn collect_birth_dates_from_individuals(
    individuals: &[Individual],
) -> HashMap<String, NaiveDate> {
    let mut birth_dates = HashMap::new();

    // Extract birth dates from individuals
    for individual in individuals {
        if let Some(birthdate) = individual.birth_date {
            birth_dates.insert(individual.pnr.clone(), birthdate);
        }
    }

    birth_dates
}

/// Registry field mapper
///
/// This trait defines an interface for mapping registry fields to Individual
/// model fields using field mappers.
pub trait RegistryFieldMapper: Send + Sync {
    /// Get the registry type name
    fn registry_type(&self) -> &str;

    /// Apply all field mappings to an individual
    ///
    /// # Arguments
    ///
    /// * `individual` - The individual to apply mappings to
    /// * `record_batch` - The record batch to extract values from
    /// * `row` - The row index to extract
    ///
    /// # Returns
    ///
    /// A Result indicating success or failure
    fn apply_mappings(
        &self,
        individual: &mut dyn Any,
        record_batch: &RecordBatch,
        row: usize,
    ) -> Result<()>;
}
