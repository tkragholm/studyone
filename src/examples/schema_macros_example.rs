//! Example demonstrating the use of schema macros
//!
//! This example shows how to use the new schema macros to define registries
//! with minimal boilerplate.

use chrono::NaiveDate;
use crate::define_registry;

// Define the VNDS registry using our new macro
define_registry! {
    name: "VNDS",
    description: "Migration registry containing migration information",
    struct VndsRegistry {
        #[field(name = "PNR", nullable = false)]
        pnr: String,

        #[field(name = "INDUD_KODE", nullable = true)]
        event_type: Option<String>,

        #[field(name = "HAEND_DATO", nullable = true)]
        event_date: Option<NaiveDate>,
    }
}

/// Run the schema macros example
pub fn run_schema_macros_example() {
    println!("Running schema macros example");

    // Create a deserializer using the macro-generated implementation
    let deserializer = VndsRegistryDeserializer::new();

    // Print deserializer info
    println!("Created deserializer for VNDS registry");
    println!("Deserializer: {:?}", deserializer);

    // In a real example, we would deserialize a record batch:
    // let batches = load_batches("path/to/vnds_data.parquet");
    // for batch in batches {
    //     let individuals = deserializer.deserialize_batch(&batch).unwrap();
    //     for individual in individuals {
    //         println!("Deserialized individual: {}", individual.pnr);
    //     }
    // }
}
