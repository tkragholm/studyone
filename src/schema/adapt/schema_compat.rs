//! Module for handling schema compatibility with adaptation support.

use std::sync::Arc;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use arrow::array::ArrayRef;

use crate::schema::adapt::types::{AdaptationStrategy, Result, AdapterError, DateFormatConfig, TypeCompatibility};
use crate::schema::adapt::compatibility::{check_type_compatibility, determine_adaptation_strategy};
use crate::schema::adapt::conversions::convert_array;

/// An enhanced schema compatibility report with adaptation information
#[derive(Debug)]
pub struct EnhancedSchemaCompatibilityReport {
    /// Whether all schemas are compatible with adaptation
    pub compatible: bool,
    /// List of incompatibility issues that cannot be resolved with adaptation
    pub issues: Vec<SchemaAdaptationIssue>,
    /// List of adaptations that will be performed
    pub adaptations: Vec<SchemaAdaptation>,
}

/// A schema compatibility issue with adaptation context
#[derive(Debug)]
pub struct SchemaAdaptationIssue {
    /// The field name with incompatibility
    pub field_name: String,
    /// The source data type
    pub source_type: DataType,
    /// The target data type
    pub target_type: DataType,
    /// Description of the incompatibility
    pub description: String,
}

/// A schema adaptation to be performed
#[derive(Debug)]
pub struct SchemaAdaptation {
    /// The field name to adapt
    pub field_name: String,
    /// The source data type
    pub source_type: DataType,
    /// The target data type
    pub target_type: DataType,
    /// The strategy for adaptation
    pub adaptation_strategy: AdaptationStrategy,
}

/// Check schema compatibility with adaptation options
#[must_use]
pub fn check_schema_with_adaptation(
    source_schema: &Schema,
    target_schema: &Schema,
) -> EnhancedSchemaCompatibilityReport {
    let mut issues = Vec::new();
    let mut adaptations = Vec::new();
    let mut all_compatible = true;

    // Check each field in the target schema
    for target_field in target_schema.fields() {
        let field_name = target_field.name();

        // Try to find matching field in source schema
        if let Ok(source_field) = source_schema.field_with_name(field_name) {
            let source_type = source_field.data_type();
            let target_type = target_field.data_type();

            // Check compatibility
            match check_type_compatibility(source_type, target_type) {
                TypeCompatibility::Exact => {
                    // Types match exactly, no adaptation needed
                }
                TypeCompatibility::Compatible => {
                    // Types need adaptation
                    let strategy = determine_adaptation_strategy(source_type, target_type);
                    adaptations.push(SchemaAdaptation {
                        field_name: field_name.to_string(),
                        source_type: source_type.clone(),
                        target_type: target_type.clone(),
                        adaptation_strategy: strategy,
                    });
                }
                TypeCompatibility::Incompatible => {
                    // Types are incompatible
                    all_compatible = false;
                    issues.push(SchemaAdaptationIssue {
                        field_name: field_name.to_string(),
                        source_type: source_type.clone(),
                        target_type: target_type.clone(),
                        description: format!(
                            "Incompatible types for field '{field_name}': {source_type:?} cannot be converted to {target_type:?}"
                        ),
                    });
                }
            }
        } else {
            // Field doesn't exist in source schema
            // This is not an adaptation issue, as we can create null values
            // But we should note it for informational purposes
            adaptations.push(SchemaAdaptation {
                field_name: field_name.to_string(),
                source_type: DataType::Null,
                target_type: target_field.data_type().clone(),
                adaptation_strategy: AdaptationStrategy::AutoCast,
            });
        }
    }

    EnhancedSchemaCompatibilityReport {
        compatible: all_compatible,
        issues,
        adaptations,
    }
}

/// Convert a record batch to match the target schema with type adaptation
pub fn adapt_record_batch(
    batch: &RecordBatch,
    target_schema: &Schema,
    date_config: &DateFormatConfig,
) -> Result<RecordBatch> {
    let source_schema = batch.schema();
    let mut adapted_columns: Vec<ArrayRef> = Vec::with_capacity(target_schema.fields().len());

    // Process each field in the target schema
    for target_field in target_schema.fields() {
        let field_name = target_field.name();
        let target_type = target_field.data_type();

        // Try to find the field in the source schema
        if let Ok(source_idx) = source_schema.index_of(field_name) {
            let source_array = batch.column(source_idx);
            let source_type = source_array.data_type();

            match check_type_compatibility(source_type, target_type) {
                TypeCompatibility::Exact => {
                    // Types match, use column as-is
                    adapted_columns.push(source_array.clone());
                }
                TypeCompatibility::Compatible => {
                    // Types need conversion
                    let converted = convert_array(source_array, target_type, date_config)?;
                    adapted_columns.push(converted);
                }
                TypeCompatibility::Incompatible => {
                    return Err(AdapterError::ValidationError(format!(
                        "Incompatible types for field '{field_name}': {source_type:?} -> {target_type:?}"
                    )));
                }
            }
        } else {
            // Field doesn't exist in source schema, create a null column
            let null_array = crate::schema::adapt::conversions::create_null_array(target_type, batch.num_rows())?;
            adapted_columns.push(null_array);
        }
    }

    // Create a new record batch with the adapted columns
    RecordBatch::try_new(Arc::new(target_schema.clone()), adapted_columns)
        .map_err(AdapterError::ArrowError)
}