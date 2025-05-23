//! Test for the Registry trait with VNDS data
//!
//! This is a simple test for the `RegistryTrait` derive macro

use chrono::NaiveDate;
use par_reader::{RegistryTrait, error, models, registry, schema};

fn main() {
    println!("VNDS Registry test");

    // Define a simple registry using the derive macro
    #[derive(RegistryTrait, Debug)]
    #[registry(name = "VNDS", description = "Migration registry", id_field = "pnr")]
    struct VndsRegistry {
        // Field mappings
        #[field(name = "PNR")]
        pnr: String,

        #[field(name = "INDUD_KODE")]
        event_type: Option<String>,

        #[field(name = "HAEND_DATO")]
        event_date: Option<NaiveDate>,
    }

    // Create a deserializer
    let deserializer = VndsRegistryDeserializer::new();

    println!(
        "Created deserializer: inner type = {}",
        deserializer.inner.registry_type()
    );
}
