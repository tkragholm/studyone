//! PNR-specific filtering utilities
//!
//! This module contains specialized functionality for filtering
//! by Danish personal identification numbers (PNR).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::Context;
use arrow::array::{Array, BooleanArray, StringArray};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use rayon::prelude::*;

use crate::error::{ParquetReaderError, Result};
use crate::filter::core::{BatchFilter, filter_record_batch};
use crate::filter::expr::{Expr, ExpressionFilter, LiteralValue};

/// A filter that includes only rows with matching PNR values
#[derive(Debug, Clone)]
pub struct PnrFilter {
    /// The set of PNR values to include
    pnr_values: HashSet<String>,

    /// The name of the PNR column
    pnr_column: String,
}

impl PnrFilter {
    /// Create a new PNR filter
    ///
    /// # Arguments
    /// * `pnr_values` - The set of PNR values to include
    /// * `pnr_column` - The name of the PNR column (defaults to "PNR")
    ///
    /// # Returns
    /// A new PNR filter
    #[must_use]
    pub fn new<S: ::std::hash::BuildHasher>(
        pnr_values: &HashSet<String, S>,
        pnr_column: Option<String>,
    ) -> Self {
        Self {
            pnr_values: pnr_values.iter().cloned().collect(),
            pnr_column: pnr_column.unwrap_or_else(|| "PNR".to_string()),
        }
    }

    /// Find the PNR column name and index in a record batch
    ///
    /// # Arguments
    /// * `batch` - The record batch to search for PNR column
    ///
    /// # Returns
    /// A tuple with the PNR column name and index
    ///
    /// # Errors
    /// Returns an error if PNR column cannot be found or accessed
    pub fn find_pnr_column(batch: &RecordBatch) -> Result<(String, usize)> {
        // Try to find the PNR column with either uppercase or lowercase
        let pnr_col_name = match batch.schema().field_with_name("PNR") {
            Ok(_) => "PNR",
            Err(_) => match batch.schema().field_with_name("pnr") {
                Ok(_) => "pnr",
                Err(_) => {
                    return Err(anyhow::anyhow!("PNR column not found in record batch"));
                }
            },
        };

        let pnr_idx = batch.schema().index_of(pnr_col_name).map_err(|e| {
            ParquetReaderError::MetadataError(format!("PNR column not found in record batch: {e}"))
        })?;

        Ok((pnr_col_name.to_string(), pnr_idx))
    }

    /// Create a boolean mask for PNR filtering
    ///
    /// # Arguments
    /// * `pnr_array` - The array containing PNR values
    /// * `pnr_filter` - The set of PNR values to filter by
    ///
    /// # Returns
    /// * `Result<BooleanArray>` - Boolean mask where true means PNR in filter
    fn create_pnr_mask<S: ::std::hash::BuildHasher>(
        &self,
        pnr_array: &StringArray,
        pnr_filter: &HashSet<String, S>,
    ) -> Result<BooleanArray> {
        // Pre-size the mask vector for efficiency
        let mut mask_values = Vec::with_capacity(pnr_array.len());

        // Optimize for different filter sizes
        if pnr_filter.len() > 10_000 {
            // For large filter sets, use hash-based lookup for each value
            for i in 0..pnr_array.len() {
                let in_filter = if pnr_array.is_null(i) {
                    false
                } else {
                    pnr_filter.contains(pnr_array.value(i))
                };
                mask_values.push(in_filter);
            }
        } else {
            // For smaller filter sets, create a HashMap for quick lookup with count of occurrences
            let mut pnr_batch_counts = HashMap::with_capacity(pnr_array.len());

            // First pass: count occurrences in the batch
            for i in 0..pnr_array.len() {
                if !pnr_array.is_null(i) {
                    let pnr = pnr_array.value(i);
                    if pnr_filter.contains(pnr) {
                        *pnr_batch_counts.entry(pnr.to_string()).or_insert(0) += 1;
                    }
                }
            }

            // Second pass: create mask using our counts
            for i in 0..pnr_array.len() {
                let in_filter = if pnr_array.is_null(i) {
                    false
                } else {
                    pnr_batch_counts.contains_key(pnr_array.value(i))
                };
                mask_values.push(in_filter);
            }
        }

        Ok(BooleanArray::from(mask_values))
    }
}

impl BatchFilter for PnrFilter {
    fn filter(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        // Find the PNR column - first try the specified column name
        let col_idx = match batch.schema().index_of(&self.pnr_column) {
            Ok(idx) => idx,
            Err(_) => {
                // If that fails, try to auto-detect
                let (_, idx) = Self::find_pnr_column(batch)?;
                idx
            }
        };

        let pnr_array = batch.column(col_idx);
        let pnr_array = pnr_array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                ParquetReaderError::MetadataError("PNR column is not a string array".to_string())
            })?;

        // Create a boolean mask using SIMD-accelerated operations where possible
        let filter_mask = self.create_pnr_mask(pnr_array, &self.pnr_values)?;

        // Apply the filter to all columns
        filter_record_batch(batch, &filter_mask)
    }

    fn required_columns(&self) -> HashSet<String> {
        let mut cols = HashSet::new();
        cols.insert(self.pnr_column.clone());
        cols
    }
}

/// Create an expression filter for PNR values
///
/// # Arguments
/// * `pnr_values` - The set of PNR values to include
/// * `pnr_column` - Optional name of the PNR column
///
/// # Returns
/// An expression filter for PNR values
#[must_use]
pub fn create_pnr_expression_filter<S: ::std::hash::BuildHasher>(
    pnr_values: &HashSet<String, S>,
    pnr_column: Option<String>,
) -> ExpressionFilter {
    let col_name = pnr_column.unwrap_or_else(|| "PNR".to_string());

    let values = pnr_values
        .iter()
        .map(|s| LiteralValue::String(s.clone()))
        .collect();

    let expr = Expr::In(col_name, values);

    ExpressionFilter::new(expr)
}

/// Join two record batches on a column and filter by PNR
///
/// This function joins a batch with PNR column to another batch that
/// needs to be filtered by PNR but doesn't contain PNR directly.
///
/// # Arguments
/// * `pnr_batch` - Batch containing PNR column
/// * `pnr_column` - Name of the PNR column in pnr_batch
/// * `join_batch` - Batch to filter
/// * `join_column` - Name of the join column in both batches
/// * `pnr_filter` - Optional set of PNR values to filter by
///
/// # Returns
/// A filtered batch with rows from join_batch that match the PNR filter
///
/// # Errors
/// Returns an error if joining or filtering fails
pub fn join_and_filter_by_pnr(
    pnr_batch: &RecordBatch,
    pnr_column: &str,
    join_batch: &RecordBatch,
    join_column: &str,
    pnr_filter: Option<&HashSet<String>>,
) -> Result<RecordBatch> {
    // Locate the columns
    let pnr_idx = pnr_batch
        .schema()
        .index_of(pnr_column)
        .with_context(|| format!("PNR column '{pnr_column}' not found"))?;

    let join_idx_pnr = pnr_batch.schema().index_of(join_column).map_err(|e| {
        ParquetReaderError::MetadataError(format!(
            "Join column '{join_column}' not found in PNR batch: {e}"
        ))
    })?;

    let join_idx = join_batch.schema().index_of(join_column).map_err(|e| {
        ParquetReaderError::MetadataError(format!(
            "Join column '{join_column}' not found in join batch: {e}"
        ))
    })?;

    // Extract arrays
    let pnr_array = pnr_batch
        .column(pnr_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            ParquetReaderError::MetadataError(format!(
                "PNR column '{pnr_column}' is not a string array"
            ))
        })?;

    let join_key_pnr = pnr_batch
        .column(join_idx_pnr)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            ParquetReaderError::MetadataError(format!(
                "Join column '{join_column}' in PNR batch is not a string array"
            ))
        })?;

    let join_key = join_batch
        .column(join_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            ParquetReaderError::MetadataError(format!(
                "Join column '{join_column}' in join batch is not a string array"
            ))
        })?;

    // Create a map of join key to PNR
    let mut join_to_pnr = HashMap::with_capacity(pnr_batch.num_rows());

    for i in 0..pnr_batch.num_rows() {
        if !pnr_array.is_null(i) && !join_key_pnr.is_null(i) {
            let pnr = pnr_array.value(i);
            let key = join_key_pnr.value(i);

            // Only include if it passes the PNR filter
            if pnr_filter.is_none() || pnr_filter.unwrap().contains(pnr) {
                join_to_pnr.insert(key.to_string(), pnr.to_string());
            }
        }
    }

    // Create a mask for the join batch
    let mut mask_values = Vec::with_capacity(join_batch.num_rows());

    for i in 0..join_batch.num_rows() {
        let in_filter = if join_key.is_null(i) {
            false
        } else {
            join_to_pnr.contains_key(join_key.value(i))
        };
        mask_values.push(in_filter);
    }

    let filter_mask = BooleanArray::from(mask_values);

    // Apply the filter to all columns
    filter_record_batch(join_batch, &filter_mask)
}

/// A filter plan for efficiently filtering data by PNR
#[derive(Debug, Clone, Default)]
pub struct FilterPlan {
    /// Registries that can be filtered directly by PNR
    direct_filters: HashMap<String, String>, // (registry, pnr_column)

    /// Registries that need to be filtered via joins
    join_filters: HashMap<String, (String, String)>, // (registry, parent_registry, parent_column)
}

impl FilterPlan {
    /// Create a new filter plan
    #[must_use]
    pub fn new() -> Self {
        Self {
            direct_filters: HashMap::new(),
            join_filters: HashMap::new(),
        }
    }

    /// Add a registry that can be filtered directly by PNR
    pub fn add_direct_filter(&mut self, registry: String, pnr_column: String) {
        self.direct_filters.insert(registry, pnr_column);
    }

    /// Add a registry that needs to be filtered via a join
    pub fn add_join_filter(
        &mut self,
        registry: String,
        parent_registry: String,
        parent_column: String,
    ) {
        self.join_filters
            .insert(registry, (parent_registry, parent_column));
    }

    /// Check if a registry is included in the plan
    #[must_use]
    pub fn has_registry(&self, registry: &str) -> bool {
        self.direct_filters.contains_key(registry) || self.join_filters.contains_key(registry)
    }

    /// Check if a registry can be filtered directly by PNR
    #[must_use]
    pub fn is_direct_filter(&self, registry: &str) -> bool {
        self.direct_filters.contains_key(registry)
    }

    /// Get the PNR column for a registry
    #[must_use]
    pub fn get_pnr_column(&self, registry: &str) -> Option<&String> {
        self.direct_filters.get(registry)
    }

    /// Get the join information for a registry
    #[must_use]
    pub fn get_join_info(&self, registry: &str) -> Option<&(String, String)> {
        self.join_filters.get(registry)
    }
}

/// Build a multi-step filter plan for efficient filtering
///
/// # Arguments
/// * `schemas` - The schemas of the registries to filter
/// * `joins` - Map of join relationships between registries
/// * `pnr_columns` - Map of PNR column names for each registry
///
/// # Returns
/// A filter plan for efficiently filtering the registries
#[must_use]
pub fn build_filter_plan(
    schemas: &HashMap<String, SchemaRef>,
    joins: &HashMap<String, (String, String)>, // (registry, join_from, join_to)
    pnr_columns: &HashMap<String, String>,
) -> FilterPlan {
    let mut plan = FilterPlan::new();

    // First, identify registries that can be filtered directly by PNR
    for (registry, schema) in schemas {
        if let Some(pnr_column) = pnr_columns.get(registry) {
            // Check if the registry has a PNR column
            if schema.field_with_name(pnr_column).is_ok() {
                plan.add_direct_filter(registry.clone(), pnr_column.clone());
            }
        }
    }

    // Then, identify registries that need to be filtered via joins
    for (registry, join_info) in joins {
        let (parent_registry, parent_column) = join_info;
        if !plan.has_registry(registry) {
            // First check if the parent registry can be directly filtered
            if plan.is_direct_filter(parent_registry) {
                plan.add_join_filter(
                    registry.clone(),
                    parent_registry.clone(),
                    parent_column.clone(),
                );
            }
        }
    }

    plan
}

/// Apply a filter plan to filter multiple record batches by PNR
///
/// # Arguments
/// * `plan` - The filter plan to apply
/// * `batches` - Map of registry name to record batches
/// * `pnr_filter` - Set of PNR values to filter by
///
/// # Returns
/// A map of registry name to filtered record batches
///
/// # Errors
/// Returns an error if filtering fails
pub fn apply_filter_plan(
    plan: &FilterPlan,
    batches: &HashMap<String, Vec<RecordBatch>>,
    pnr_filter: &HashSet<String>,
) -> Result<HashMap<String, Vec<RecordBatch>>> {
    let mut filtered_batches = HashMap::with_capacity(batches.len());

    // Create a PNR filter once and reuse it
    let _pnr_filter_obj = Arc::new(PnrFilter::new(pnr_filter, None));

    // First, filter registries that can be filtered directly by PNR
    for (registry, pnr_column) in &plan.direct_filters {
        if let Some(registry_batches) = batches.get(registry) {
            // Create a PNR filter with the specified column name
            let registry_filter =
                Arc::new(PnrFilter::new(pnr_filter, Some(pnr_column.clone())));

            // Filter each batch in parallel
            let filtered: Result<Vec<RecordBatch>> = registry_batches
                .par_iter()
                .map(|batch| registry_filter.filter(batch))
                .collect();

            // Store non-empty batches
            let result = filtered?;
            if !result.is_empty() {
                filtered_batches.insert(registry.clone(), result);
            }
        }
    }

    // Then, filter registries that need to be filtered via joins
    for (registry, (parent_registry, join_column)) in &plan.join_filters {
        // Use the same column name for both parent and child
        if let Some(registry_batches) = batches.get(registry) {
            if let Some(parent_batches) = filtered_batches.get(parent_registry) {
                // This is a simplification - in a real implementation, you'd need
                // to handle joining across multiple batches more carefully
                if let Some(parent_batch) = parent_batches.first() {
                    if let Some(pnr_column) = plan.get_pnr_column(parent_registry) {
                        let filtered: Result<Vec<RecordBatch>> = registry_batches
                            .par_iter()
                            .map(|batch| {
                                join_and_filter_by_pnr(
                                    parent_batch,
                                    pnr_column,
                                    batch,
                                    join_column,
                                    Some(pnr_filter),
                                )
                            })
                            .collect();

                        let result = filtered?;
                        if !result.is_empty() {
                            filtered_batches.insert(registry.clone(), result);
                        }
                    }
                }
            }
        }
    }

    Ok(filtered_batches)
}
