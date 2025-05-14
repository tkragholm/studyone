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

### Current Issue

In `trait_deserializer.rs`, the `deserialize_row` method returns `None` when the Individual's `pnr` field is empty:

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

    // Return the deserialized Individual if it has a valid PNR
    if individual.pnr.is_empty() {
        Ok(None)  // <-- THIS LINE causes issues for non-PNR registries
    } else {
        Ok(Some(individual))
    }
}
```

For registry types like LPR_DIAG that use record_number instead of PNR, this means that `deserialize_row` always returns `None`.

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