#!/usr/bin/env bash

# Fix errors in the codebase related to error handling

# 1. Fix the most common errors
find src -name "*.rs" -type f -exec gsed -i '' \
    -e 's/Error::IoError(\(.*\))/anyhow::anyhow!("IO error: {}", \1)/g' \
    -e 's/Error::ParquetError(\(.*\))/anyhow::anyhow!("Parquet error: {}", \1)/g' \
    -e 's/Error::ArrowError(\(.*\))/anyhow::anyhow!("Arrow error: {}", \1)/g' \
    -e 's/Error::SchemaError(\(.*\))/anyhow::anyhow!("Schema error: {}", \1)/g' \
    -e 's/Error::MetadataError(\(.*\))/anyhow::anyhow!("Metadata error: {}", \1)/g' \
    -e 's/Error::FilterError(\(.*\))/anyhow::anyhow!("Filter error: {}", \1)/g' \
    -e 's/Error::ValidationError(\(.*\))/anyhow::anyhow!("Validation error: {}", \1)/g' \
    -e 's/Error::InvalidOperation(\(.*\))/anyhow::anyhow!("Invalid operation: {}", \1)/g' \
    -e 's/ParquetReaderError::IoError(\(.*\))/anyhow::anyhow!("IO error: {}", \1)/g' \
    -e 's/ParquetReaderError::ParquetError(\(.*\))/anyhow::anyhow!("Parquet error: {}", \1)/g' \
    -e 's/ParquetReaderError::ArrowError(\(.*\))/anyhow::anyhow!("Arrow error: {}", \1)/g' \
    -e 's/ParquetReaderError::SchemaError(\(.*\))/anyhow::anyhow!("Schema error: {}", \1)/g' \
    -e 's/ParquetReaderError::MetadataError(\(.*\))/anyhow::anyhow!("Metadata error: {}", \1)/g' \
    -e 's/ParquetReaderError::FilterError(\(.*\))/anyhow::anyhow!("Filter error: {}", \1)/g' \
    -e 's/ParquetReaderError::ValidationError(\(.*\))/anyhow::anyhow!("Validation error: {}", \1)/g' \
    -e 's/ParquetReaderError::InvalidOperation(\(.*\))/anyhow::anyhow!("Invalid operation: {}", \1)/g' \
    {} \;

# 2. Add missing context pattern
find src -name "*.rs" -type f -exec gsed -i '' \
    -e 's/\.map_err(|e| .*Error::ArrowError(format!("\(.*\): {e}")))/\.context("\1")/g' \
    -e 's/\.map_err(|e| .*Error::IoError(format!("\(.*\): {e}")))/\.context("\1")/g' \
    -e 's/\.map_err(|e| .*Error::ParquetError(format!("\(.*\): {e}")))/\.context("\1")/g' \
    -e 's/\.map_err(|e| .*Error::SchemaError(format!("\(.*\): {e}")))/\.context("\1")/g' \
    -e 's/\.map_err(|e| .*Error::MetadataError(format!("\(.*\): {e}")))/\.context("\1")/g' \
    -e 's/\.map_err(|e| .*Error::FilterError(format!("\(.*\): {e}")))/\.context("\1")/g' \
    -e 's/\.map_err(|e| .*Error::ValidationError(format!("\(.*\): {e}")))/\.context("\1")/g' \
    -e 's/\.map_err(|e| .*Error::InvalidOperation(format!("\(.*\): {e}")))/\.context("\1")/g' \
    {} \;

echo "Script completed. Check 'cargo check' for remaining errors."
