# Implementation Plan for TODO Items

This document outlines the approach for implementing the tasks listed in TODO.txt.

## 1. Generating Registry Traits Directly from Schema Definitions

### Current Architecture
- Schemas are defined in registry-specific schema.rs files
- Registry deserializers are implemented using the `generate_trait_deserializer!` macro
- Field extractors handle converting Arrow data to Rust types
- Setter closures apply the extracted values to the Individual model

### Proposed Improvements
1. **Create a proc macro for generating registry traits**:
   ```rust
   #[derive(RegistryTrait)]
   #[registry(name = "VNDS", description = "Migration registry")]
   struct VndsRegistry {
       #[field(name = "PNR", type = "String", nullable = false)]
       pnr: String,
       
       #[field(name = "INDUD_KODE", type = "String", nullable = true)]
       migration_code: Option<String>,
       
       #[field(name = "HAEND_DATO", type = "Date", nullable = true)]
       event_date: Option<NaiveDate>,
   }
   ```

2. **Make the proc macro generate**:
   - Field definitions
   - Arrow schema
   - Extractors
   - Deserializer implementation
   - RegistryDeserializer trait implementation

## 2. Using Rust Attributes/Derive Macros to Tie Field Definitions to Struct Fields

### Current Architecture
- Field definitions are separate from model structs
- Setters are implemented as closures that mutate the Individual model
- There's no direct mapping between schema fields and struct fields

### Proposed Improvements
1. **Create a FieldMapping derive macro**:
   ```rust
   #[derive(FieldMapping)]
   struct DiagnosisModel {
       #[field(registry = "LPR", name = "DIAGKODE", type = "String")]
       diagnosis_code: String,
       
       #[field(registry = "LPR", name = "DIAG_TYPE", type = "String")]
       diagnosis_type: String,
   }
   ```

2. **Generate bidirectional mapping code**:
   - From registry data to model fields
   - From model fields to registry data (for potential writing)
   - Type conversion and validation

3. **Support for multiple registries**:
   - Allow fields to specify which registry they belong to
   - Generate appropriate mapping code for each registry

## 3. Automating Creation of Registry-Specific Methods Through Code Generation

### Current Architecture
- Registry-specific methods are implemented manually
- There's no automated way to generate specialized methods for each registry type

### Proposed Improvements
1. **Create a trait macro for generating registry-specific methods**:
   ```rust
   #[registry_methods(registry = "LPR")]
   trait LprMethods {
       fn get_diagnoses(&self) -> Vec<Diagnosis>;
       fn has_diagnosis(&self, code: &str) -> bool;
   }
   ```

2. **Auto-generate implementations based on registry schema**:
   - Generate specific accessor methods for each registry
   - Implement specialized filtering and validation logic
   - Add registry-specific conversion methods

3. **Create registry-specific type-safe builders**:
   ```rust
   let lpr_query = LprQueryBuilder::new()
       .with_diagnosis_code("I21")
       .with_admission_date_after(date)
       .build();
   ```

## Implementation Strategy

1. **Phase 1: Schema-based Trait Generation**
   - Implement a proc macro to generate registry traits from schema definitions
   - Update existing code to use the new trait generation system
   - Add tests to verify type safety and correctness

2. **Phase 2: Field Mapping Automation**
   - Create the FieldMapping derive macro
   - Implement bidirectional mappings
   - Add support for field validation

3. **Phase 3: Registry-Specific Methods Generation**
   - Implement the registry_methods macro
   - Create specialized query builders for each registry
   - Add high-level API for common operations

## Support for Different ID Field Types (LPR Implementation Fix)

The current trait deserializer system has been enhanced to support different ID field types. However, an issue in the base `RegistryDeserializer` trait implementation prevents it from working fully with non-PNR identifiers.

### ID Field Validation Issue (Fixed)

Previously, in `trait_deserializer.rs`, the `deserialize_row` method would only check if the Individual's `pnr` field was empty, which caused issues for non-PNR registries. This has been fixed by implementing proper ID field type validation.

The updated version now checks the appropriate ID field based on the registry type:

```rust
fn deserialize_row(&self, batch: &RecordBatch, row: usize) -> Result<Option<Individual>> {
    // Create a new Individual with empty values
    let mut individual = Individual::new(
        String::new(), // Empty PNR to be filled by extractors
        None,          // No birth date yet
    );

    // Apply all field extractors
    for extractor in self.field_extractors() {
        extractor.extract_and_set(batch, row, &mut individual as &mut dyn Any)?;
    }

    // Check appropriate ID field based on registry type
    let has_valid_id = match self.id_field_type() {
        "pnr" => !individual.pnr.is_empty(),
        "record_number" => individual.properties()
            .and_then(|props| props.get("record_number"))
            .and_then(|v| v.downcast_ref::<Option<String>>())
            .and_then(|v| v.as_ref().map(|s| !s.is_empty()))
            .unwrap_or(false),
        "dw_ek_kontakt" => individual.properties()
            .and_then(|props| props.get("dw_ek_kontakt"))
            .and_then(|v| v.downcast_ref::<Option<String>>())
            .and_then(|v| v.as_ref().map(|s| !s.is_empty()))
            .unwrap_or(false),
        _ => !individual.pnr.is_empty(), // Default to checking PNR for backward compatibility
    };

    if has_valid_id {
        Ok(Some(individual))
    } else {
        Ok(None)
    }
}
```

### Property Mapping Issue (Fixed)

We identified and fixed two related issues in how properties are handled between Individual and registry-specific types:

1. **ID Field Issue**: For non-PNR ID fields like "record_number" (used by LPR_DIAG), the StringExtractor was correctly extracting the "RECNUM" value from the Arrow record batch, but it wasn't properly setting it as the "record_number" property that the ID field validation checks for.

2. **Field Value Consistency Issue**: Fields like birth_date, gender, diagnosis_code, event_type, and event_date were being extracted correctly but not properly stored in the properties map for access during conversion back to registry-specific types.

The issues were fixed by:

1. **Updating Field Mapping**: Modified the field mapping in `macros/src/lib.rs` to properly map source field names (e.g., "RECNUM") to the standardized property names (e.g., "record_number") used in ID field validation.

2. **Enhancing StringExtractor**: Updated the `StringExtractor.extract_and_set` method in `src/registry/extractors.rs` to handle special ID fields, ensuring that when extracting the "RECNUM" field, it's also stored with the standardized property name "record_number".

3. **Universal Property Storage**: Modified `Individual.set_property` to use a more systematic approach where all fields are stored in both their dedicated fields and in the properties map, ensuring consistency for all `From<Individual>` implementations.

4. **Improved Property Access**: Enhanced the `From<Individual>` implementation to more robustly check for properties using both the field name and stringified field name.

This solution ensures that all registry types, regardless of their ID field type or field naming convention, properly store and retrieve values from the Individual model during deserialization and conversion.

### Temporary Workaround

A manual extraction approach has been implemented in the example code that works correctly:

1. The ADM records are processed first to build a mapping from RECNUM to PNR
2. The DIAG records are manually extracted from the Arrow batch
3. For each DIAG record, we check if its RECNUM exists in our mapping
4. If it does, we create a Diagnosis object with the PNR from the mapping

### Long-Term Solution

1. **Update the `RegistryDeserializer` trait**:
   - Modify the trait to be aware of which field is being used as the identifier
   - Update the `deserialize_row` method to check the appropriate ID field instead of always checking `pnr`

```rust
fn deserialize_row(&self, batch: &RecordBatch, row: usize) -> Result<Option<Individual>> {
    // Create a new Individual with empty values
    let mut individual = Individual::new(
        String::new(), 
        None,
    );

    // Apply all field extractors
    for extractor in self.field_extractors() {
        extractor.extract_and_set(batch, row, &mut individual as &mut dyn Any)?;
    }

    // Check appropriate ID field based on registry type
    let has_valid_id = match self.id_field_type() {
        "pnr" => !individual.pnr.is_empty(),
        "record_number" => individual.properties()
            .and_then(|props| props.get("record_number"))
            .and_then(|v| v.downcast_ref::<Option<String>>())
            .and_then(|v| v.as_ref().map(|s| !s.is_empty()))
            .unwrap_or(false),
        "dw_ek_kontakt" => individual.properties()
            .and_then(|props| props.get("dw_ek_kontakt"))
            .and_then(|v| v.downcast_ref::<Option<String>>())
            .and_then(|v| v.as_ref().map(|s| !s.is_empty()))
            .unwrap_or(false),
        _ => !individual.pnr.is_empty(), // Default to checking PNR
    };

    if has_valid_id {
        Ok(Some(individual))
    } else {
        Ok(None)
    }
}
```

2. **Expand the proc macro**:
   - Add an attribute to specify the identifier field
   - Generate the appropriate trait implementation based on the identifier type
   - Ensure the ID field is correctly handled in all generated code

```rust
#[derive(RegistryTrait)]
#[registry(name = "LPR_DIAG", description = "LPR Diagnosis registry", id_field = "record_number")]
struct LprDiagRegistry {
    #[field(name = "RECNUM")]
    record_number: Option<String>,
    
    #[field(name = "C_DIAG")]
    diagnosis_code: Option<String>,
}
```

## Benefits

1. **Improved Type Safety**
   - Compile-time validation of registry field access
   - Automatic type conversions
   - Clear error messages for invalid field access

2. **Reduced Boilerplate**
   - Eliminate repetitive code in schema definitions
   - Auto-generate deserializers and mappings
   - Simplified maintenance and updates

3. **Better Developer Experience**
   - IDE autocompletion for registry-specific methods
   - Clear documentation through code structure
   - Fewer runtime errors due to improved type safety

## Recommendations for Property System Improvement

The current property system in the Individual model maintains two storage mechanisms:
1. Dedicated fields for type-safe access to common properties (e.g., pnr, birth_date, gender)
2. A dynamic properties map (`HashMap<String, Box<dyn Any + Send + Sync>>`) for extensibility

While our current fix ensures values are always stored in both locations, there are opportunities for further improvements:

### 1. Leveraging Registry Traits with Proc Macros

The codebase already defines registry-specific traits (`BefFields`, `LprFields`, `VndsFields`, etc.) that provide type-safe interfaces. We should better leverage these traits in the property system:

1. **Full Trait Implementation**: Ensure the Individual model fully implements all registry traits, providing type-safe access methods.

2. **Trait-Based Conversion**: Modify the proc macro-generated `From<Individual>` implementations to use the trait methods instead of directly accessing the properties map:

   ```rust
   // Current approach (directly accessing properties map)
   if let Some(props) = individual.properties() {
       if let Some(value) = props.get("event_type") {
           if let Some(string_val) = value.downcast_ref::<Option<String>>() {
               instance.event_type = string_val.clone();
           }
       }
   }
   
   // Improved approach (using trait methods)
   if let Some(vnds_fields) = as_vnds_fields(&individual) {
       instance.event_type = vnds_fields.event_type().map(String::from);
   }
   ```

### 2. Property Reflection System

Develop a more robust property reflection system that automatically handles synchronization between dedicated fields and the properties map:

1. **PropertyReflection Trait**: Define a trait that abstracts the process of reflecting values between dedicated fields and the properties map.

2. **Automatic Field Reflection**: Use proc macros to generate field reflection code that eliminates the need for manual property handling in the `set_property` method.

3. **Type-Safe Property Access**: Provide generic type-safe wrappers around property access that eliminate the need for manual downcasting.

### 3. Unified Registry Trait

Consider creating a unified registry trait that combines elements from all registry-specific traits:

1. **Common Interface**: Define a common interface for accessing fields across all registry types.

2. **Registry Type Detection**: Add methods to detect which registry type an Individual instance represents.

3. **Registry Type Conversion**: Provide methods for converting between registry types when possible.

By implementing these improvements, we can create a more robust, type-safe, and maintainable system for handling properties, making the codebase easier to work with and less prone to runtime errors.