//! Individual entity model
//!
//! This module contains the core Individual entity structure which is central to the study design.
//! An Individual represents any person in the study, and can be associated with various roles
//! such as parent or child.

use crate::error::Result;
use arrow::array::Array;
use arrow::array::{BooleanArray, Date32Array, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{Datelike, NaiveDate};

/// Gender of an individual
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Gender {
    /// Male gender
    Male,
    /// Female gender
    Female,
    /// Unknown or not specified
    Unknown,
}

impl From<&str> for Gender {
    fn from(s: &str) -> Self {
        match s.trim().to_lowercase().as_str() {
            "m" | "male" | "1" => Self::Male,
            "f" | "female" | "2" => Self::Female,
            _ => Self::Unknown,
        }
    }
}

impl From<i32> for Gender {
    fn from(value: i32) -> Self {
        match value {
            1 => Self::Male,
            2 => Self::Female,
            _ => Self::Unknown,
        }
    }
}

/// Geographic origin category
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Origin {
    /// Danish origin
    Danish,
    /// Western immigrant or descendant
    Western,
    /// Non-western immigrant or descendant
    NonWestern,
    /// Unknown origin
    Unknown,
}

impl From<&str> for Origin {
    fn from(s: &str) -> Self {
        match s.trim().to_lowercase().as_str() {
            "danish" | "danmark" | "dk" | "1" => Self::Danish,
            "western" | "west" | "2" => Self::Western,
            "non-western" | "nonwestern" | "3" => Self::NonWestern,
            _ => Self::Unknown,
        }
    }
}

impl From<i32> for Origin {
    fn from(value: i32) -> Self {
        match value {
            1 => Self::Danish,
            2 => Self::Western,
            3 => Self::NonWestern,
            _ => Self::Unknown,
        }
    }
}

/// Education level using ISCED classification
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EducationLevel {
    /// ISCED 0-2: Low education
    Low,
    /// ISCED 3-4: Medium education
    Medium,
    /// ISCED 5-8: High education
    High,
    /// Unknown education level
    Unknown,
}

impl From<&str> for EducationLevel {
    fn from(s: &str) -> Self {
        match s.trim().to_lowercase().as_str() {
            "low" | "1" => Self::Low,
            "medium" | "2" => Self::Medium,
            "high" | "3" => Self::High,
            _ => Self::Unknown,
        }
    }
}

impl From<i32> for EducationLevel {
    fn from(value: i32) -> Self {
        match value {
            1 => Self::Low,
            2 => Self::Medium,
            3 => Self::High,
            _ => Self::Unknown,
        }
    }
}

/// Core Individual entity representing a person in the study
#[derive(Debug, Clone)]
pub struct Individual {
    /// Personal identification number (PNR)
    pub pnr: String,
    /// Gender of the individual
    pub gender: Gender,
    /// Birth date
    pub birth_date: Option<NaiveDate>,
    /// Death date, if applicable
    pub death_date: Option<NaiveDate>,
    /// Geographic origin category
    pub origin: Origin,
    /// Education level
    pub education_level: EducationLevel,
    /// Municipality code at index date
    pub municipality_code: Option<String>,
    /// Whether the individual lives in a rural area
    pub is_rural: bool,
    /// Mother's PNR, if known
    pub mother_pnr: Option<String>,
    /// Father's PNR, if known
    pub father_pnr: Option<String>,
    /// Family identifier
    pub family_id: Option<String>,
    /// Emigration date, if applicable
    pub emigration_date: Option<NaiveDate>,
    /// Immigration date, if applicable
    pub immigration_date: Option<NaiveDate>,
}

impl Individual {
    /// Create a new Individual with minimal required information
    #[must_use]
    pub const fn new(pnr: String, gender: Gender, birth_date: Option<NaiveDate>) -> Self {
        Self {
            pnr,
            gender,
            birth_date,
            death_date: None,
            origin: Origin::Unknown,
            education_level: EducationLevel::Unknown,
            municipality_code: None,
            is_rural: false,
            mother_pnr: None,
            father_pnr: None,
            family_id: None,
            emigration_date: None,
            immigration_date: None,
        }
    }
    
    /// Create an Individual directly from a BEF schema record
    ///
    /// This constructor understands the BEF registry schema and can extract
    /// appropriate fields to create an Individual object. It handles field
    /// extraction, type conversion, and default values automatically.
    ///
    /// # Arguments
    ///
    /// * `batch` - The record batch with BEF schema
    /// * `row` - The row index to extract
    ///
    /// # Returns
    ///
    /// * `Result<Individual>` - The created Individual or an error
    pub fn from_bef_record(batch: &RecordBatch, row: usize) -> Result<Self> {
        use crate::utils::array_utils::{downcast_array, get_column};
        use arrow::datatypes::DataType;
        use arrow::array::{Array, Date32Array, Int8Array, StringArray};
        
        // Extract required columns with schema adaptation
        let pnr_array_opt = get_column(batch, "PNR", &DataType::Utf8, true)?;
        
        // Need to get the column array directly
        let pnr_array = match &pnr_array_opt {
            Some(array) => downcast_array::<StringArray>(array, "PNR", "String")?,
            None => return Err(crate::error::Error::ColumnNotFound {
                column: "PNR".to_string(),
            }.into()),
        };
        
        // Extract birth date
        let birth_day_array_opt = get_column(batch, "FOED_DAG", &DataType::Date32, true)?;
        
        // Extract parent IDs
        let far_id_array_opt = get_column(batch, "FAR_ID", &DataType::Utf8, false)?;
        let mor_id_array_opt = get_column(batch, "MOR_ID", &DataType::Utf8, false)?;
        
        // Get family ID
        let familie_id_array_opt = get_column(batch, "FAMILIE_ID", &DataType::Utf8, false)?;
        
        // Get gender
        let gender_array_opt = get_column(batch, "KOEN", &DataType::Utf8, false)?;
        
        // Get municipality code
        let municipality_array_opt = get_column(batch, "KOM", &DataType::Int8, false)?;
        
        // Get origin information (optional)
        let origin_array_opt = get_column(batch, "OPR_LAND", &DataType::Utf8, false)?;
        
        let pnr = pnr_array.value(row).to_string();
        
        // Determine gender
        let gender = if let Some(array) = &gender_array_opt {
            let gender_array = downcast_array::<StringArray>(array, "KOEN", "String")?;
            if row < gender_array.len() && !gender_array.is_null(row) {
                match gender_array.value(row) {
                    "M" => Gender::Male,
                    "F" => Gender::Female,
                    _ => Gender::Unknown
                }
            } else {
                Gender::Unknown
            }
        } else {
            Gender::Unknown
        };
        
        // Extract birth date
        let birth_date = if let Some(array) = &birth_day_array_opt {
            let date32_array = downcast_array::<Date32Array>(array, "FOED_DAG", "Date32")?;
            if row < date32_array.len() && !date32_array.is_null(row) {
                let days = date32_array.value(row);
                NaiveDate::from_ymd_opt(1970, 1, 1)
                    .and_then(|epoch| epoch.checked_add_days(chrono::Days::new(days as u64)))
            } else {
                None
            }
        } else {
            None
        };
        
        // Parse origin
        let origin = if let Some(array) = &origin_array_opt {
            let origin_array = downcast_array::<StringArray>(array, "OPR_LAND", "String")?;
            if row < origin_array.len() && !origin_array.is_null(row) {
                match origin_array.value(row) {
                    "5100" => Origin::Danish, // Denmark
                    code => {
                        let code_num = code.parse::<i32>().unwrap_or(0);
                        if (5000..5999).contains(&code_num) {
                            // Western countries
                            Origin::Western
                        } else {
                            Origin::NonWestern
                        }
                    }
                }
            } else {
                Origin::Danish // Default to Danish when null
            }
        } else {
            Origin::Danish // Default to Danish when column is missing
        };
        
        // Create the base individual
        let mut individual = Individual::new(pnr, gender, birth_date);
        
        // Set origin
        individual.origin = origin;
        
        // Set education level (not available in BEF)
        individual.education_level = EducationLevel::Unknown;
        
        // Set municipality code
        if let Some(array) = &municipality_array_opt {
            let municipality_array = downcast_array::<Int8Array>(array, "KOM", "Int8")?;
            if row < municipality_array.len() && !municipality_array.is_null(row) {
                individual.municipality_code = Some(municipality_array.value(row).to_string());
                // Municipalities with codes below 100 are typically rural
                individual.is_rural = municipality_array.value(row) < 100;
            }
        }
        
        // Set parent PNRs
        if let Some(array) = &far_id_array_opt {
            let far_id_array = downcast_array::<StringArray>(array, "FAR_ID", "String")?;
            if row < far_id_array.len() && !far_id_array.is_null(row) {
                individual.father_pnr = Some(far_id_array.value(row).to_string());
            }
        }
        
        if let Some(array) = &mor_id_array_opt {
            let mor_id_array = downcast_array::<StringArray>(array, "MOR_ID", "String")?;
            if row < mor_id_array.len() && !mor_id_array.is_null(row) {
                individual.mother_pnr = Some(mor_id_array.value(row).to_string());
            }
        }
        
        // Set family ID
        if let Some(array) = &familie_id_array_opt {
            let familie_id_array = downcast_array::<StringArray>(array, "FAMILIE_ID", "String")?;
            if row < familie_id_array.len() && !familie_id_array.is_null(row) {
                individual.family_id = Some(familie_id_array.value(row).to_string());
            }
        }
        
        Ok(individual)
    }
    
    /// Create a collection of Individuals from a BEF record batch
    ///
    /// # Arguments
    ///
    /// * `batch` - The record batch with BEF schema
    ///
    /// # Returns
    ///
    /// * `Result<Vec<Individual>>` - The created Individuals or an error
    pub fn from_bef_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        let mut individuals = Vec::with_capacity(batch.num_rows());
        
        for i in 0..batch.num_rows() {
            individuals.push(Self::from_bef_record(batch, i)?);
        }
        
        Ok(individuals)
    }
    
    // Helper function to create optimized batch conversions for other registry types
    // Add additional from_*_record and from_*_batch methods for other registries
    
    /// Calculate age of the individual at a specific reference date
    #[must_use]
    pub fn age_at(&self, reference_date: &NaiveDate) -> Option<i32> {
        match self.birth_date {
            Some(birth_date) => {
                // Check if the individual was alive at the reference date
                if self.death_date.is_none_or(|d| d >= *reference_date) {
                    let years = reference_date.year() - birth_date.year();
                    // Adjust for birthday not yet reached in the reference year
                    if reference_date.month() < birth_date.month()
                        || (reference_date.month() == birth_date.month()
                            && reference_date.day() < birth_date.day())
                    {
                        Some(years - 1)
                    } else {
                        Some(years)
                    }
                } else {
                    // Individual was not alive at the reference date
                    None
                }
            }
            None => None,
        }
    }

    /// Check if the individual was alive at a specific date
    #[must_use]
    pub fn was_alive_at(&self, date: &NaiveDate) -> bool {
        // Check birth date (must be born before or on the date)
        if let Some(birth) = self.birth_date {
            if birth > *date {
                return false;
            }
        } else {
            // Unknown birth date, can't determine
            return false;
        }

        // Check death date (must not have died before the date)
        if let Some(death) = self.death_date {
            if death < *date {
                return false;
            }
        }

        true
    }

    /// Check if the individual was resident in Denmark at a specific date
    #[must_use]
    pub fn was_resident_at(&self, date: &NaiveDate) -> bool {
        // Must be alive to be resident
        if !self.was_alive_at(date) {
            return false;
        }

        // Check emigration status
        if let Some(emigration) = self.emigration_date {
            if emigration <= *date {
                // Check if they immigrated back after emigration
                if let Some(immigration) = self.immigration_date {
                    return immigration > emigration && immigration <= *date;
                }
                return false;
            }
        }

        // Either never emigrated or emigrated after the date
        true
    }

    /// Get the Arrow schema for Individual records
    #[must_use]
    pub fn schema() -> Schema {
        Schema::new(vec![
            Field::new("pnr", DataType::Utf8, false),
            Field::new("gender", DataType::Int32, false),
            Field::new("birth_date", DataType::Date32, true),
            Field::new("death_date", DataType::Date32, true),
            Field::new("origin", DataType::Int32, false),
            Field::new("education_level", DataType::Int32, false),
            Field::new("municipality_code", DataType::Utf8, true),
            Field::new("is_rural", DataType::Boolean, false),
            Field::new("mother_pnr", DataType::Utf8, true),
            Field::new("father_pnr", DataType::Utf8, true),
            Field::new("family_id", DataType::Utf8, true),
            Field::new("emigration_date", DataType::Date32, true),
            Field::new("immigration_date", DataType::Date32, true),
        ])
    }

    /// Convert a `RecordBatch` to a vector of Individual objects
    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        let pnr_array = batch
            .column(batch.schema().index_of("pnr")?)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let gender_array = batch
            .column(batch.schema().index_of("gender")?)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        let birth_date_array = batch
            .column(batch.schema().index_of("birth_date")?)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();

        let death_date_array = batch
            .column(batch.schema().index_of("death_date")?)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();

        let origin_array = batch
            .column(batch.schema().index_of("origin")?)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        let education_level_array = batch
            .column(batch.schema().index_of("education_level")?)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        let municipality_code_array = batch
            .column(batch.schema().index_of("municipality_code")?)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let is_rural_array = batch
            .column(batch.schema().index_of("is_rural")?)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();

        let mother_pnr_array = batch
            .column(batch.schema().index_of("mother_pnr")?)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let father_pnr_array = batch
            .column(batch.schema().index_of("father_pnr")?)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let family_id_array = batch
            .column(batch.schema().index_of("family_id")?)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let emigration_date_array = batch
            .column(batch.schema().index_of("emigration_date")?)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();

        let immigration_date_array = batch
            .column(batch.schema().index_of("immigration_date")?)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();

        let mut individuals = Vec::with_capacity(batch.num_rows());

        for i in 0..batch.num_rows() {
            let individual = Self {
                pnr: pnr_array.value(i).to_string(),
                gender: Gender::from(gender_array.value(i)),
                birth_date: if birth_date_array.is_null(i) {
                    None
                } else {
                    // Convert Date32 to NaiveDate (days since Unix epoch)
                    Some(
                        NaiveDate::from_ymd_opt(1970, 1, 1)
                            .unwrap()
                            .checked_add_days(chrono::Days::new(birth_date_array.value(i) as u64))
                            .unwrap(),
                    )
                },
                death_date: if death_date_array.is_null(i) {
                    None
                } else {
                    Some(
                        NaiveDate::from_ymd_opt(1970, 1, 1)
                            .unwrap()
                            .checked_add_days(chrono::Days::new(death_date_array.value(i) as u64))
                            .unwrap(),
                    )
                },
                origin: Origin::from(origin_array.value(i)),
                education_level: EducationLevel::from(education_level_array.value(i)),
                municipality_code: if municipality_code_array.is_null(i) {
                    None
                } else {
                    Some(municipality_code_array.value(i).to_string())
                },
                is_rural: is_rural_array.value(i),
                mother_pnr: if mother_pnr_array.is_null(i) {
                    None
                } else {
                    Some(mother_pnr_array.value(i).to_string())
                },
                father_pnr: if father_pnr_array.is_null(i) {
                    None
                } else {
                    Some(father_pnr_array.value(i).to_string())
                },
                family_id: if family_id_array.is_null(i) {
                    None
                } else {
                    Some(family_id_array.value(i).to_string())
                },
                emigration_date: if emigration_date_array.is_null(i) {
                    None
                } else {
                    Some(
                        NaiveDate::from_ymd_opt(1970, 1, 1)
                            .unwrap()
                            .checked_add_days(chrono::Days::new(
                                emigration_date_array.value(i) as u64
                            ))
                            .unwrap(),
                    )
                },
                immigration_date: if immigration_date_array.is_null(i) {
                    None
                } else {
                    Some(
                        NaiveDate::from_ymd_opt(1970, 1, 1)
                            .unwrap()
                            .checked_add_days(chrono::Days::new(
                                immigration_date_array.value(i) as u64
                            ))
                            .unwrap(),
                    )
                },
            };

            individuals.push(individual);
        }

        Ok(individuals)
    }

    /// Convert a vector of Individual objects to a `RecordBatch`
    pub fn to_record_batch(_individuals: &[Self]) -> Result<RecordBatch> {
        // Implementation of conversion to RecordBatch
        // This would create Arrow arrays for each field and then combine them
        // For brevity, this is left as a placeholder
        unimplemented!("Conversion to RecordBatch not yet implemented")
    }
}
