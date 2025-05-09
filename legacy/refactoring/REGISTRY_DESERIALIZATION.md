# Registry Deserialization Architecture

This document explains the refactored registry deserialization architecture, which clarifies the boundary between domain models and registry-specific data conversion.

## Design Principles

The refactoring follows these key principles:

1. **Clear Module Boundaries**:
   - `models/core`: Defines domain models and their behavior
   - `registry`: Handles all registry-specific conversion logic
   - `serde`: Provides serialization/deserialization utilities

2. **Single Responsibility per Module**:
   - Models define "what" data they contain
   - Registry modules define "how" to extract that data from registries

3. **Direct Deserialization**:
   - Use `SerdeIndividual` for efficient direct conversion
   - Minimize manual field extraction

## Module Structure

```
src/
├── models/
│   └── core/
│       └── individual/
│           ├── base.rs                # Core Individual definition
│           ├── registry_integration.rs # Simplified registry integration API
│           └── serde.rs               # Serde wrapper for Individual
└── registry/
    ├── detect.rs                      # Registry type detection
    ├── deserializer.rs                # Central deserializer interface
    ├── bef/
    │   ├── deserializer.rs            # BEF-specific deserialization
    │   └── ...
    ├── ind/
    │   ├── deserializer.rs            # IND-specific deserialization
    │   └── ...
    └── ...
```

## Key Components

### Registry Detection

The `registry/detect.rs` module defines a unified way to detect registry types:

```rust
pub enum RegistryType {
    BEF, IND, LPR, MFR, VNDS, DOD, AKM, UDDF, Unknown
}

pub fn detect_registry_type(batch: &RecordBatch) -> RegistryType
```

### Central Deserializer

The `registry/deserializer.rs` module provides a unified interface:

```rust
pub fn deserialize_batch(batch: &RecordBatch) -> Result<Vec<Individual>>
pub fn deserialize_row(batch: &RecordBatch, row: usize) -> Result<Option<Individual>>
```

This delegates to registry-specific deserializers based on the detected registry type.

### Registry-Specific Deserializers

Each registry submodule (`bef`, `ind`, etc.) contains a `deserializer.rs` that implements:

```rust
pub fn deserialize_batch(batch: &RecordBatch) -> Result<Vec<Individual>>
pub fn deserialize_row(batch: &RecordBatch, row: usize) -> Result<Option<Individual>>
```

These use `SerdeIndividual` to efficiently convert registry data to domain models.

### Individual Registry Integration

The `models/core/individual/registry_integration.rs` module provides a simplified API:

```rust
impl Individual {
    pub fn enhance_from_registry(&mut self, batch: &RecordBatch, row: usize) -> Result<bool>
    pub fn pnr_matches_record(&self, batch: &RecordBatch, row: usize) -> Result<bool>
}
```

This delegates the actual implementation to the registry module.

## Implementation Details

### SerdeIndividual

The existing `SerdeIndividual` wrapper is used for direct deserialization:

```rust
pub struct SerdeIndividual {
    #[serde(with = "IndividualDef")]
    inner: Individual,
}

// Field mappings using serde attributes
#[serde(remote = "Individual")]
struct IndividualDef {
    #[serde(alias = "PNR")]
    pnr: String,
    // ...
}
```

### Field Mapping

Field mappings are defined in the schema modules:

```rust
// In registry/bef/schema.rs
pub fn field_mapping() -> HashMap<String, String> {
    let mut mapping = HashMap::new();
    mapping.insert("PNR".to_string(), "pnr".to_string());
    // ...
    mapping
}
```

And used by deserializers:

```rust
// In registry/bef/deserializer.rs
pub fn field_mapping() -> HashMap<String, String> {
    schema::field_mapping()
}
```

## Usage Examples

### Loading Individuals from Registry Data

```rust
// Load a batch from any registry
let batch = some_registry.load("/path/to/data", None)?;

// Deserialize into Individuals
let individuals = registry::deserializer::deserialize_batch(&batch)?;
```

### Enhancing an Existing Individual

```rust
// Create or get an Individual
let mut individual = Individual::new("1234567890", Gender::Unknown, None);

// Enhance with registry data
let batch = some_registry.load("/path/to/data", None)?;
individual.enhance_from_registry(&batch, 0)?;
```

## Benefits of the Refactoring

1. **Clear Responsibilities**: Each module has a focused and well-defined purpose
2. **Reduced Duplication**: Registry-specific logic is centralized
3. **Improved Efficiency**: Direct deserialization instead of manual field extraction
4. **Better Maintainability**: Adding a new registry type just requires a new deserializer module
5. **Type Safety**: Registry types are represented as proper enums

## Future Enhancements

1. Implement deserializers for all registry types
2. Add more comprehensive field mapping
3. Optimize batch deserialization performance further
4. Add registry-specific validation rules