//! Test for the Registry trait with BEF data
//!
//! This is a simple test for the `RegistryTrait` derive macro with BEF data

use chrono::NaiveDate;
use par_reader::{RegistryTrait, error, models, registry, schema};

fn main() {
    println!("BEF Registry test");

    // Define BEF Registry using the derive macro
    #[derive(RegistryTrait, Debug)]
    #[registry(name = "BEF", description = "Population registry")]
    struct BefRegistry {
        // Core identification fields
        #[field(name = "PNR")]
        pnr: String,

        // Fields from BEF registry
        #[field(name = "KOEN")]
        gender: Option<String>,

        #[field(name = "FOED_DAG")]
        birth_date: Option<NaiveDate>,

        #[field(name = "MOR_ID")]
        mother_pnr: Option<String>,

        #[field(name = "FAR_ID")]
        father_pnr: Option<String>,

        #[field(name = "FAMILIE_ID")]
        family_id: Option<String>,
    }

    // Create deserializer
    let deserializer = BefRegistryDeserializer::new();

    // Print deserializer info
    println!(
        "Created deserializer for {} registry",
        deserializer.inner.registry_type()
    );

    println!("BEF Registry test completed successfully!");
}
