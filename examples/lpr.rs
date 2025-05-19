//! Test for the Registry trait with LPR data
//!
//! This is a simple test for the `RegistryTrait` derive macro with LPR data

use chrono::NaiveDate;
use par_reader::{RegistryTrait, error, models, registry, schema};

fn main() {
    println!("LPR Registry test");

    // Define LPR ADM Registry using the derive macro
    #[derive(RegistryTrait, Debug)]
    #[registry(name = "LPR_ADM", description = "LPR Administrative registry")]
    struct LprAdmRegistry {
        // Core identification fields
        #[field(name = "PNR")]
        pnr: String,

        // Admission-related fields
        #[field(name = "C_ADIAG")]
        action_diagnosis: Option<String>,

        #[field(name = "C_AFD")]
        department_code: Option<String>,

        #[field(name = "C_KOM")]
        municipality_code: Option<String>,

        #[field(name = "D_INDDTO")]
        admission_date: Option<NaiveDate>,

        #[field(name = "D_UDDTO")]
        discharge_date: Option<NaiveDate>,

        #[field(name = "V_ALDER")]
        _age: Option<i32>,

        #[field(name = "V_SENGDAGE")]
        _length_of_stay: Option<i32>,
    }

    // Define LPR DIAG Registry using the derive macro
    #[derive(RegistryTrait, Debug)]
    #[registry(name = "LPR_DIAG", description = "LPR Diagnosis registry")]
    struct LprDiagRegistry {
        // Core identification fields
        #[field(name = "PNR")]
        pnr: String,

        // Diagnosis fields
        #[field(name = "C_DIAG")]
        diagnosis_code: Option<String>,

        #[field(name = "C_DIAGTYPE")]
        diagnosis_type: Option<String>,

        #[field(name = "RECNUM")]
        record_number: Option<String>,
    }

    // Create deserializers for both registry types
    let adm_deserializer = LprAdmRegistryDeserializer::new();
    let diag_deserializer = LprDiagRegistryDeserializer::new();

    // Print deserializer info
    println!(
        "Created deserializer for {} registry",
        adm_deserializer.inner.registry_type()
    );
    println!(
        "Created deserializer for {} registry",
        diag_deserializer.inner.registry_type()
    );

    println!("LPR Registries test completed successfully!");
}
