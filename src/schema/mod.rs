//! Module for handling parquet file schema compatibility.

use parquet::schema::types::Type;

// Re-export old adapters module for backward compatibility
//pub mod adapters;
// New modular design
pub mod adapt;

// Re-export the main adaptation types and functions for easier access
pub use adapt::{
    AdaptationStrategy, AdapterError, DateFormatConfig, TypeCompatibility, adapt_record_batch,
    check_schema_with_adaptation, convert_array,
};

/// A struct that represents the compatibility between parquet file schemas
#[derive(Debug)]
pub struct SchemaCompatibilityReport {
    /// Whether all schemas are compatible
    pub compatible: bool,
    /// List of incompatibility issues, if any
    pub issues: Vec<SchemaIssue>,
}

/// A schema compatibility issue
#[derive(Debug)]
pub struct SchemaIssue {
    /// The path of the file that has incompatible schema
    pub file_path: String,
    /// The reference file path being compared to
    pub reference_path: String,
    /// Description of the incompatibility
    pub description: String,
}

/// Checks if two schemas are compatible for merging datasets
#[must_use]
pub fn schemas_compatible(schema1: &Type, schema2: &Type) -> bool {
    // For simplicity, we check that the schema names and structures are identical
    // In a real-world scenario, you might want to implement a more sophisticated
    // compatibility check depending on your use case

    // Check name
    if schema1.name() != schema2.name() {
        return false;
    }

    // Skip repetition check for now to avoid assertion errors
    // with malformed parquet files
    // This is a defensive approach that allows reading more files
    // without breaking on schema validation

    // Check physical type only for primitive types
    if schema1.is_primitive() && schema2.is_primitive() {
        if schema1.get_physical_type() != schema2.get_physical_type() {
            return false;
        }
    } else if schema1.is_primitive() != schema2.is_primitive() {
        // One is primitive and one isn't
        return false;
    }

    // For group types, check children
    if schema1.is_group() && schema2.is_group() {
        let fields1 = schema1.get_fields();
        let fields2 = schema2.get_fields();

        if fields1.len() != fields2.len() {
            return false;
        }

        for (f1, f2) in fields1.iter().zip(fields2.iter()) {
            if !schemas_compatible(f1, f2) {
                return false;
            }
        }
    }

    true
}

/// Finds and returns detailed incompatibilities between two schemas
#[must_use]
pub fn find_schema_incompatibilities(
    schema1: &Type,
    schema2: &Type,
    reference_path: &str,
    file_path: &str,
) -> Vec<SchemaIssue> {
    let mut issues = Vec::new();

    if schema1.name() != schema2.name() {
        issues.push(SchemaIssue {
            file_path: file_path.to_string(),
            reference_path: reference_path.to_string(),
            description: format!(
                "Schema name mismatch: '{}' vs '{}'",
                schema1.name(),
                schema2.name()
            ),
        });
    }

    // Compare field types and names for struct types (most common case)
    let fields1 = schema1.get_fields();
    let fields2 = schema2.get_fields();
    if !fields1.is_empty() && !fields2.is_empty() {
        if fields1.len() != fields2.len() {
            issues.push(SchemaIssue {
                file_path: file_path.to_string(),
                reference_path: reference_path.to_string(),
                description: format!(
                    "Different number of fields: {} vs {}",
                    fields1.len(),
                    fields2.len()
                ),
            });
            return issues; // Early return as field count mismatch makes further comparisons difficult
        }

        // Compare each field
        for (i, (f1, f2)) in fields1.iter().zip(fields2.iter()).enumerate() {
            if f1.name() != f2.name() {
                issues.push(SchemaIssue {
                    file_path: file_path.to_string(),
                    reference_path: reference_path.to_string(),
                    description: format!(
                        "Field name mismatch at position {}: '{}' vs '{}'",
                        i,
                        f1.name(),
                        f2.name()
                    ),
                });
            }

            // Check for type compatibility
            if !types_compatible(f1, f2) {
                let type_description = if f1.is_primitive() && f2.is_primitive() {
                    format!(
                        "Field type mismatch for '{}': {:?} vs {:?}",
                        f1.name(),
                        f1.get_physical_type(),
                        f2.get_physical_type()
                    )
                } else if f1.is_primitive() {
                    format!(
                        "Field type mismatch for '{}': primitive vs group",
                        f1.name()
                    )
                } else {
                    format!(
                        "Field type mismatch for '{}': group vs primitive",
                        f1.name()
                    )
                };

                issues.push(SchemaIssue {
                    file_path: file_path.to_string(),
                    reference_path: reference_path.to_string(),
                    description: type_description,
                });
            }

            // Recursively check nested fields
            if f1.is_group() && f2.is_group() {
                let nested_issues =
                    find_schema_incompatibilities(f1, f2, reference_path, file_path);
                issues.extend(nested_issues);
            }
        }
    }

    issues
}

/// Checks if two field types are compatible
#[must_use]
pub fn types_compatible(field1: &Type, field2: &Type) -> bool {
    // Skip repetition check for now to avoid assertion errors
    // with malformed parquet files
    // This is a defensive approach that allows reading more files
    // without breaking on schema validation

    // Check if both are the same kind (primitive or group)
    if field1.is_primitive() != field2.is_primitive() {
        return false;
    }

    // For primitive types, check physical type
    if field1.is_primitive()
        && field2.is_primitive()
        && field1.get_physical_type() != field2.get_physical_type()
    {
        return false;
    }

    // For group types, we'd check children structure,
    // but that's handled by the recursive schema comparison

    true
}
