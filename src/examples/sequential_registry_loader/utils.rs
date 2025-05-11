use arrow::array::StringArray;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use crate::models::core::types::SocioeconomicStatus;
use crate::registry::{bef, death, ind, mfr, vnds};
use arrow::array::Array;

/// Map SOCIO code values to SocioeconomicStatus enum
pub fn map_socio_to_enum(socio_value: i32) -> SocioeconomicStatus {
    // Map SOCIO values (which might be encoded differently) to the standard enum values
    // Actual mapping may need adjustment based on the specific encoding in the data
    match socio_value {
        110..=129 => SocioeconomicStatus::SelfEmployedWithEmployees, // Self-employed with employees
        120..=139 => SocioeconomicStatus::SelfEmployedWithoutEmployees, // Self-employed without employees
        310..=330 => SocioeconomicStatus::TopLevelEmployee,             // Top-level employees
        410..=440 => SocioeconomicStatus::MediumLevelEmployee,          // Medium-level employees
        510..=550 => SocioeconomicStatus::BasicLevelEmployee,           // Basic-level employees
        910..=929 => SocioeconomicStatus::Unemployed,                   // Unemployed
        500..=599 => SocioeconomicStatus::OtherEmployee,                // Other employees
        700..=799 => SocioeconomicStatus::Student,                      // Students
        800..=899 => SocioeconomicStatus::Pensioner,                    // Pensioners/retirees
        900..=999 => SocioeconomicStatus::OtherInactive, // Other economically inactive
        _ => SocioeconomicStatus::Unknown,
    }
}

/// Get a string value from a record batch column at the specified row index
pub fn get_string_value(batch: &RecordBatch, column_name: &str, row: usize) -> Option<String> {
    batch
        .column_by_name(column_name)
        .and_then(|col| col.as_any().downcast_ref::<StringArray>())
        .and_then(|array| {
            if row < array.len() && !array.is_null(row) {
                Some(array.value(row).to_string())
            } else {
                None
            }
        })
}

/// Get the schema for a specific registry type
pub fn get_registry_schema(registry: &str) -> Option<Arc<arrow::datatypes::Schema>> {
    match registry.to_uppercase().as_str() {
        "BEF" => Some(bef::schema::bef_schema()),
        "MFR" => Some(mfr::schema::mfr_schema()),
        "IND" => Some(ind::schema::ind_schema()),
        "VNDS" => Some(vnds::schema::vnds_schema()),
        "DOD" => Some(death::dod::schema::dod_schema()),
        // Add other schemas as needed
        _ => None,
    }
}
