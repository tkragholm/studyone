# Improving Parquet File Loading for Mismatched Data Types

## Current Situation

The current implementation for loading parquet files has the following limitations when handling mismatched data types:

1. When the schema's expected data type (e.g., UTF8) doesn't match what's in the parquet file (e.g., Date32), the code will fail.
2. Schema compatibility checks are strict and reject files when types don't exactly match.
3. There's no automatic adaptation of data types between compatible formats.
4. Users have to manually modify their schemas or data to ensure type compatibility.
5. Different date formats in string fields can't be properly converted to date types.

## Proposed Improvements

### 1. Create a Data Type Adaptation Module

Create a new module `src/schema/adapters.rs` that:

- Defines compatible data type conversions
- Implements utilities to convert between data types
- Provides automatic type adaptation based on predefined compatibility rules

```rust
// Example of core adapter functionality

/// Types of data type compatibility
pub enum TypeCompatibility {
    /// Types match exactly
    Exact,
    /// Types can be automatically converted
    Compatible,
    /// Types are incompatible
    Incompatible,
}

/// Check if two Arrow data types are compatible for conversion
pub fn check_type_compatibility(from: &DataType, to: &DataType) -> TypeCompatibility {
    match (from, to) {
        // Exact matches
        (a, b) if a == b => TypeCompatibility::Exact,
        
        // Compatible types
        (DataType::Int32, DataType::Int64) |
        (DataType::Int32, DataType::Float64) |
        (DataType::Int64, DataType::Float64) |
        (DataType::Float32, DataType::Float64) => TypeCompatibility::Compatible,
        
        // String-convertible types
        (DataType::Utf8, DataType::Date32) |
        (DataType::Utf8, DataType::Timestamp(_, _)) |
        (DataType::Date32, DataType::Utf8) |
        (DataType::Timestamp(_, _), DataType::Utf8) => TypeCompatibility::Compatible,
        
        // Incompatible types
        _ => TypeCompatibility::Incompatible,
    }
}
```

### 2. Implement Schema Checking with Type Adaptation

Enhance the schema validation to support automatic type adaptation:

```rust
pub struct EnhancedSchemaCompatibilityReport {
    pub compatible: bool,
    pub issues: Vec<SchemaIssue>,
    pub adaptations: Vec<SchemaAdaptation>,
}

pub struct SchemaAdaptation {
    pub field_name: String,
    pub source_type: DataType,
    pub target_type: DataType,
    pub adaptation_strategy: AdaptationStrategy,
}

pub enum AdaptationStrategy {
    AutoCast,
    DateParsing,
    StringConversion,
    // Add more strategies as needed
}

// Function to check schema compatibility with adaptation
pub fn check_schema_with_adaptation(file_schema: &Schema, expected_schema: &Schema) -> EnhancedSchemaCompatibilityReport {
    // Implementation details...
}
```

### 3. Create Batch Conversion Utilities

Implement utilities to convert record batches to match expected schemas:

```rust
/// Convert a record batch to match the expected schema
pub fn adapt_record_batch(batch: &RecordBatch, target_schema: &Schema) -> Result<RecordBatch> {
    let source_schema = batch.schema();
    let mut adapted_columns: Vec<ArrayRef> = Vec::with_capacity(target_schema.fields().len());
    
    for (i, target_field) in target_schema.fields().iter().enumerate() {
        let target_name = target_field.name();
        let target_type = target_field.data_type();
        
        // Try to find matching field in source schema
        match source_schema.field_with_name(target_name) {
            Ok(source_field) => {
                let source_type = source_field.data_type();
                let source_idx = source_schema.index_of(target_name)?;
                let source_array = batch.column(source_idx);
                
                match check_type_compatibility(source_type, target_type) {
                    TypeCompatibility::Exact => {
                        // Types match, use as-is
                        adapted_columns.push(source_array.clone());
                    },
                    TypeCompatibility::Compatible => {
                        // Types need conversion
                        let converted = convert_array(source_array, target_type)?;
                        adapted_columns.push(converted);
                    },
                    TypeCompatibility::Incompatible => {
                        return Err(Error::ValidationError(
                            format!("Incompatible types for field '{}': {:?} -> {:?}", 
                                target_name, source_type, target_type)
                        ).into());
                    }
                }
            },
            Err(_) => {
                // Field doesn't exist in source, add null column
                let null_array = create_null_array(target_type, batch.num_rows())?;
                adapted_columns.push(null_array);
            }
        }
    }
    
    RecordBatch::try_new(Arc::new(target_schema.clone()), adapted_columns)
        .map_err(|e| Error::ArrowError(format!("Failed to create adapted batch: {}", e)).into())
}
```

### 4. Implement Array Type Conversion Functions

Add utilities to convert between array types:

```rust
/// Convert an array from one type to another
pub fn convert_array(array: &ArrayRef, target_type: &DataType) -> Result<ArrayRef> {
    let source_type = array.data_type();
    
    match (source_type, target_type) {
        // String to Date32 conversion
        (DataType::Utf8, &DataType::Date32) => {
            convert_string_to_date32(array)
        },
        
        // Date32 to String conversion
        (&DataType::Date32, DataType::Utf8) => {
            convert_date32_to_string(array)
        },
        
        // Numeric type conversions
        (_, _) if is_numeric(source_type) && is_numeric(target_type) => {
            // Use Arrow's cast functionality for numeric conversions
            kernels::cast::cast(array, target_type)
                .map_err(|e| Error::ArrowError(format!("Failed to cast numeric type: {}", e)).into())
        },
        
        // Default case - use Arrow cast when possible
        (_, _) => {
            match kernels::cast::cast(array, target_type) {
                Ok(casted) => Ok(casted),
                Err(e) => Err(Error::ArrowError(format!(
                    "Failed to convert from {:?} to {:?}: {}", source_type, target_type, e
                )).into())
            }
        }
    }
}

// Helper for string to date32 conversion
fn convert_string_to_date32(array: &ArrayRef) -> Result<ArrayRef> {
    // Implementation that parses date strings into Date32 values
}

// Helper for date32 to string conversion
fn convert_date32_to_string(array: &ArrayRef) -> Result<ArrayRef> {
    // Implementation that formats dates as strings
}
```

### 5. Flexible Date Format Handling

Implement robust date parsing with support for multiple formats:

```rust
/// Configuration for date format handling
#[derive(Debug, Clone)]
pub struct DateFormatConfig {
    /// List of date format strings to try when parsing dates
    pub date_formats: Vec<String>,
    /// Default date format to use when converting dates to strings
    pub default_format: String,
    /// Enable heuristic format detection
    pub enable_format_detection: bool,
}

impl Default for DateFormatConfig {
    fn default() -> Self {
        Self {
            date_formats: vec![
                "%Y-%m-%d".to_string(),       // ISO format: 2023-01-15
                "%d-%m-%Y".to_string(),       // European: 15-01-2023
                "%m/%d/%Y".to_string(),       // US: 01/15/2023
                "%d/%m/%Y".to_string(),       // UK: 15/01/2023
                "%d.%m.%Y".to_string(),       // German/Danish: 15.01.2023
                "%Y%m%d".to_string(),         // Compact: 20230115
                "%d %b %Y".to_string(),       // 15 Jan 2023
                "%d %B %Y".to_string(),       // 15 January 2023
            ],
            default_format: "%Y-%m-%d".to_string(),
            enable_format_detection: true,
        }
    }
}

/// Parse date strings with multiple format attempts
pub fn parse_date_string(s: &str, config: &DateFormatConfig) -> Option<NaiveDate> {
    // First try all the provided formats
    for format in &config.date_formats {
        if let Ok(date) = NaiveDate::parse_from_str(s, format) {
            return Some(date);
        }
    }
    
    // If enabled, try heuristic detection for common formats not in the list
    if config.enable_format_detection {
        // Try to detect format based on string patterns
        if let Some(detected_format) = detect_date_format(s) {
            if let Ok(date) = NaiveDate::parse_from_str(s, &detected_format) {
                return Some(date);
            }
        }
    }
    
    None
}

/// Enhanced string to date32 conversion with multiple format support
pub fn convert_string_to_date32_with_formats(
    array: &ArrayRef, 
    date_config: &DateFormatConfig
) -> Result<ArrayRef> {
    let string_array = array.as_any().downcast_ref::<StringArray>()
        .ok_or_else(|| Error::ValidationError("Expected StringArray".to_string()))?;
    
    // Create a builder for Date32Array
    let mut date_builder = arrow::array::Date32Builder::new();
    
    // Process each string value
    for i in 0..string_array.len() {
        if string_array.is_null(i) {
            date_builder.append_null();
            continue;
        }
        
        let date_str = string_array.value(i);
        
        // Try to parse the date string using multiple formats
        match parse_date_string(date_str, date_config) {
            Some(date) => {
                // Convert to days since epoch (1970-01-01)
                let days = (date - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32;
                date_builder.append_value(days);
            },
            None => {
                // If parsing fails, log a warning and append null
                log::warn!("Failed to parse date string: '{}', setting to null", date_str);
                date_builder.append_null();
            }
        }
    }
    
    Ok(Arc::new(date_builder.finish()))
}

/// Try to detect date format based on string patterns
fn detect_date_format(s: &str) -> Option<String> {
    // Example implementation:
    
    // Check for ISO-like format with dashes
    if s.len() == 10 && s.chars().nth(4) == Some('-') && s.chars().nth(7) == Some('-') {
        return Some("%Y-%m-%d".to_string());
    }
    
    // Check for slashes
    if s.contains('/') {
        let parts: Vec<&str> = s.split('/').collect();
        if parts.len() == 3 {
            if parts[0].len() == 4 {
                return Some("%Y/%m/%d".to_string()); // YYYY/MM/DD
            } else if parts[2].len() == 4 {
                // Check if first part is likely day or month
                let first_num = parts[0].parse::<u8>().ok()?;
                if first_num > 12 {
                    return Some("%d/%m/%Y".to_string()); // DD/MM/YYYY
                } else {
                    // Could be either MM/DD/YYYY or DD/MM/YYYY
                    // Default to European format, but you might want context-specific logic here
                    return Some("%d/%m/%Y".to_string());
                }
            }
        }
    }
    
    // Check for dots
    if s.contains('.') {
        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() == 3 && parts[2].len() == 4 {
            return Some("%d.%m.%Y".to_string()); // DD.MM.YYYY
        }
    }
    
    // No recognized format
    None
}
```

### 6. Update `read_parquet` Function

Modify the read_parquet function to incorporate type adaptation:

```rust
pub fn read_parquet<S: ::std::hash::BuildHasher + std::marker::Sync>(
    path: &Path,
    schema: Option<&Schema>,
    pnr_filter: Option<&HashSet<String, S>>,
    adapt_types: bool,
    date_config: Option<&DateFormatConfig>,
) -> Result<Vec<RecordBatch>> {
    // Existing implementation...
    
    // Use default date format config if none provided
    let date_config = date_config.unwrap_or(&DateFormatConfig::default());
    
    // If schema is provided and type adaptation is enabled
    let reader = if let Some(schema) = schema {
        let file_schema = reader_builder.schema();
        
        // Check for schema compatibility with adaptation
        if adapt_types {
            let adaptation_report = check_schema_with_adaptation(file_schema, schema);
            // Log adaptations for transparency
            for adaptation in &adaptation_report.adaptations {
                log::info!(
                    "Field '{}' type adaptation: {:?} -> {:?} using strategy {:?}",
                    adaptation.field_name, adaptation.source_type, 
                    adaptation.target_type, adaptation.adaptation_strategy
                );
            }
        }
        
        // Rest of the implementation...
    }
    
    // Apply type adaptation to batches if needed
    let batches = if adapt_types && schema.is_some() {
        // Convert each batch to match the expected schema
        let target_schema = schema.unwrap();
        let adapted_batches = Vec::with_capacity(batch_results.len());
        
        for batch_result in batch_results {
            let batch = batch_result?;
            let adapted_batch = adapt_record_batch_with_date_formats(&batch, target_schema, date_config)?;
            adapted_batches.push(adapted_batch);
        }
        
        adapted_batches
    } else {
        // Original implementation...
    }
    
    // Rest of the implementation...
}
```

### 7. Add Configuration Options

Update the main configuration to include type adaptation and date format settings:

```rust
#[derive(Debug, Clone)]
pub struct ParquetReaderConfig {
    // Existing fields...
    
    /// Enable automatic data type adaptation when schemas don't match
    pub adapt_types: bool,
    
    /// Strict mode for type adaptation (fail on incompatible types)
    pub strict_adaptation: bool,
    
    /// Log all type adaptations for debugging
    pub log_adaptations: bool,
    
    /// Date format configuration for string-to-date conversions
    pub date_format_config: DateFormatConfig,
}

impl Default for ParquetReaderConfig {
    fn default() -> Self {
        Self {
            // Existing defaults...
            adapt_types: true,
            strict_adaptation: false,
            log_adaptations: true,
            date_format_config: DateFormatConfig::default(),
        }
    }
}
```

## Benefits

1. **Improved Robustness**: The system will handle more real-world data where types don't exactly match.
2. **Better User Experience**: Users won't need to manually fix type mismatches.
3. **Transparency**: All type adaptations are logged, providing clear information about what happened.
4. **Configurability**: Users can enable or disable type adaptation as needed.
5. **Graceful Degradation**: Even with type mismatches, the system will attempt to read and process the data.
6. **Flexible Date Handling**: Support for multiple date formats enhances interoperability with various data sources.

## Implementation Strategy

1. Start with the core type adaptation utilities
2. Implement basic type conversions (numeric, string/date)
3. Add flexible date format parsing with common formats
4. Enhance the schema compatibility checks
5. Update the read_parquet function
6. Add configuration options
7. Add comprehensive testing with mismatched schemas and various date formats

This approach allows for incremental improvement and testing at each step.