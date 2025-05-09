# Using Serde Attributes for Registry Conversion

This document outlines the improved approach for registry data conversion using serde attributes, which reduces code duplication and eliminates the need for post-processing.

## Problem Statement

The original approach for converting registry data to domain models had several issues:

1. **Redundant mapping code** in both model and registry modules
2. **Need for post-processing** after serde_arrow deserialization
3. **Complex field mapping** with temporary fields like "gender_code"
4. **Duplicated conversion logic** in multiple files

## Improved Approach with Serde Attributes

The new approach uses serde attributes directly in the model structs:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Individual {
    /// Personal identification number (PNR)
    #[serde(alias = "PNR")]
    pub pnr: String,
    
    /// Gender of the individual
    #[serde(alias = "KOEN", deserialize_with = "deserialize_gender")]
    pub gender: Gender,
    
    // ... other fields with appropriate attributes
}
```

### Key Benefits

1. **Direct Field Mapping**: Use `#[serde(alias = "REGISTRY_FIELD")]` to map registry field names to model fields
2. **Type Conversion**: Use `deserialize_with` to handle custom type conversions (strings to enums, etc.)
3. **No Post-Processing**: All transformations happen during deserialization
4. **Centralized Logic**: Conversion logic lives in the model where it belongs
5. **Self-Documenting**: The structure makes field mappings explicit and visible

### Implementation Details

1. **Field Aliases**: Map registry field names directly in the struct definition
   ```rust
   #[serde(alias = "PNR")]
   pub pnr: String,
   ```

2. **Custom Deserializers**: Handle complex conversions inline
   ```rust
   #[serde(alias = "KOEN", deserialize_with = "deserialize_gender")]
   pub gender: Gender,
   ```

3. **Conversion Functions**: Implement specific conversion logic
   ```rust
   fn deserialize_gender<'de, D>(deserializer: D) -> Result<Gender, D::Error>
   where
       D: Deserializer<'de>,
   {
       let gender_code = String::deserialize(deserializer)?;
       Ok(match gender_code.as_str() {
           "M" => Gender::Male,
           "F" => Gender::Female,
           _ => Gender::Unknown,
       })
   }
   ```

## Migration Path

To migrate from the previous approach:

1. Replace field mapping hashmap with serde aliases
2. Convert post-processing logic to custom deserializers
3. Remove temporary field mappings
4. Update registry converters to use the new model directly

## Example

### Old Approach:
```rust
// Field mapping in registry converter
fn field_mapping() -> HashMap<String, String> {
    let mut mapping = HashMap::new();
    mapping.insert("PNR".to_string(), "pnr".to_string());
    mapping.insert("KOEN".to_string(), "gender_code".to_string());
    // ... more mappings
    mapping
}

// Post-processing in registry converter
fn post_process_individual(individual: &mut Individual) {
    // Convert gender code to Gender enum
    if let Some(gender_code) = individual.extract_field::<String>("gender_code") {
        individual.gender = match gender_code.as_str() {
            "M" => Gender::Male,
            "F" => Gender::Female,
            _ => Gender::Unknown,
        };
    }
    // ... more post-processing
}
```

### New Approach:
```rust
// Direct field mapping in model
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Individual {
    #[serde(alias = "PNR")]
    pub pnr: String,
    
    #[serde(alias = "KOEN", deserialize_with = "deserialize_gender")]
    pub gender: Gender,
    
    // ... other fields
}

// Custom deserializer for complex types
fn deserialize_gender<'de, D>(deserializer: D) -> Result<Gender, D::Error>
where
    D: Deserializer<'de>,
{
    let gender_code = String::deserialize(deserializer)?;
    Ok(match gender_code.as_str() {
        "M" => Gender::Male,
        "F" => Gender::Female,
        _ => Gender::Unknown,
    })
}
```

## Conclusion

This approach significantly simplifies the registry conversion code by:

1. **Eliminating post-processing** need
2. **Centralizing conversion logic** in the model
3. **Making registry field mappings explicit** and visible in the model
4. **Improving type safety** with custom deserializers
5. **Reducing code duplication** between model and registry modules

It's a more idiomatic approach to serialization/deserialization in Rust and leverages serde's powerful attribute system to handle the complexity directly in the model definition.