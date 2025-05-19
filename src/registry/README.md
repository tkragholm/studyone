# Registry Direct Deserialization

This module provides a direct approach to deserialize registry data into Individual models without requiring intermediate registry-specific structs.

## How It Works

The direct deserialization approach works through field mappings that define:

1. **Field Definitions**: What is the name and type of the field in the source data
2. **Extractors**: How to extract values from the source data (e.g., from Arrow columns)
3. **Setters**: How to set values on the target Individual model

Each registry has its own schema that defines these mappings. For example:

```rust
FieldMapping::new(
    FieldDefinition::new("SOCIO13", "socioeconomic_status", FieldType::String, true),
    Extractors::string("SOCIO13"),
    ModelSetters::string_setter(|individual, value| {
        // Convert to i32 if possible, otherwise store as None
        if let Ok(status_code) = value.parse::<i32>() {
            individual.socioeconomic_status = Some(status_code);
        }
    }),
)
```

This mapping:
- Looks for a field named `SOCIO13` in the source data 
- Extracts it as a string
- Sets the `socioeconomic_status` property on the Individual model

## Registry Schemas

Each registry has its own schema with mappings appropriate for that registry:

- **VNDS**: Migration registry with event type and date
- **BEF**: Population registry with demographic information
- **AKM**: Labour registry with socioeconomic status

## Using the Deserializer

To use the direct deserializer:

```rust
// Create a deserializer for the specified registry
let deserializer = DirectIndividualDeserializer::new("AKM");

// Deserialize a record batch
let individuals = deserializer.deserialize_batch(&batch)?;
```

## Adding New Registry Support

To add support for a new registry:

1. Add a new schema creation method in `direct_deserializer.rs`
2. Add your registry name to the match statement in the `new` method
3. Create field mappings for each field you want to extract

## Future Work

In the future, we should create a macro that automatically generates these field mappings from registry trait definitions. This would reduce duplication and make it easier to add new registries.

Instead of manually writing:
```rust
FieldMapping::new(
    FieldDefinition::new("PNR", "pnr", FieldType::PNR, false),
    Extractors::string("PNR"),
    ModelSetters::string_setter(|individual, value| {
        individual.pnr = value;
    }),
)
```

We could derive it from:
```rust
#[derive(RegistryTrait, Debug)]
#[registry(name = "AKM", description = "Labour register")]
pub struct AkmRegistry {
    /// Person ID (CPR number)
    #[field(name = "PNR")]
    pub pnr: String,
    
    /// Socioeconomic status code
    #[field(name = "SOCIO13")]
    pub socioeconomic_status: Option<String>,
}
```