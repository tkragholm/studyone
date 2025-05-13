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