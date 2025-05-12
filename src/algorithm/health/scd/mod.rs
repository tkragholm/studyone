//! Severe Chronic Disease (SCD) algorithm implementation
//!
//! This module implements the Severe Chronic Disease (SCD) algorithm for
//! identifying patients with severe chronic diseases based on ICD-10 diagnosis codes.

pub mod categories;
pub mod severity;

use crate::error::Result;
use crate::models::health::diagnosis::{Diagnosis, DiagnosisCollection, ScdResult};
use categories::ScdCategory;
use chrono::NaiveDate;
use severity::SeverityLevel;
use std::collections::HashMap;
use std::sync::Arc;

/// Configuration for SCD algorithm
#[derive(Debug, Clone)]
pub struct ScdConfig {
    /// Start date for study period
    pub start_date: Option<NaiveDate>,
    /// End date for study period
    pub end_date: Option<NaiveDate>,
    /// Whether to include congenital diseases as SCD
    pub include_congenital: bool,
    /// Minimum age for SCD diagnosis (in years)
    pub min_age_years: Option<u32>,
    /// Maximum age for SCD diagnosis (in years)
    pub max_age_years: Option<u32>,
}

impl Default for ScdConfig {
    fn default() -> Self {
        Self {
            start_date: None,
            end_date: None,
            include_congenital: true,
            min_age_years: None,
            max_age_years: None,
        }
    }
}

/// Apply the SCD algorithm to a diagnosis collection and generate SCD results for each individual
pub fn apply_scd_algorithm(
    diagnosis_collection: &DiagnosisCollection,
    config: &ScdConfig,
    birth_dates: &HashMap<String, NaiveDate>,
) -> Result<HashMap<String, ScdResult>> {
    // Store and track SCD results by PNR
    let mut scd_results: HashMap<String, ScdResult> = HashMap::new();

    // Process each individual's diagnoses
    for pnr in diagnosis_collection.individuals_with_diagnoses() {
        // Get all diagnoses for this individual
        let diagnoses = diagnosis_collection.get_diagnoses(&pnr);

        // Skip if no diagnoses
        if diagnoses.is_empty() {
            continue;
        }

        // Create a new SCD result for this individual
        let mut scd_result = ScdResult::new(pnr.clone());

        // Process each diagnosis
        for diagnosis in &diagnoses {
            process_diagnosis(&mut scd_result, diagnosis, config, birth_dates)?;
        }

        // Add hospitalization count (this would come from a separate source,
        // for now we'll just use the diagnosis count as a proxy)
        let hospitalization_count = diagnoses.len() as i32;
        scd_result.hospitalization_count = hospitalization_count;

        // Store the result
        scd_results.insert(pnr, scd_result);
    }

    Ok(scd_results)
}

/// Process a single diagnosis and update the SCD result accordingly
fn process_diagnosis(
    scd_result: &mut ScdResult,
    diagnosis: &Arc<Diagnosis>,
    config: &ScdConfig,
    birth_dates: &HashMap<String, NaiveDate>,
) -> Result<()> {
    // Skip if outside date range
    if let (Some(date), Some(start_date)) = (diagnosis.diagnosis_date, config.start_date) {
        if date < start_date {
            return Ok(());
        }
    }

    if let (Some(date), Some(end_date)) = (diagnosis.diagnosis_date, config.end_date) {
        if date > end_date {
            return Ok(());
        }
    }

    // Skip if outside age range
    if let (Some(date), Some(birth_date)) =
        (diagnosis.diagnosis_date, birth_dates.get(&scd_result.pnr))
    {
        let age_at_diagnosis = (date - *birth_date).num_days() / 365;

        if let Some(min_age) = config.min_age_years {
            if age_at_diagnosis < i64::from(min_age) {
                return Ok(());
            }
        }

        if let Some(max_age) = config.max_age_years {
            if age_at_diagnosis > i64::from(max_age) {
                return Ok(());
            }
        }
    }

    // Determine if this is an SCD diagnosis based on ICD-10 code
    if let Some((category, is_congenital, _severity)) =
        categorize_diagnosis(&diagnosis.diagnosis_code)
    {
        // Skip congenital diseases if not included
        if is_congenital && !config.include_congenital {
            return Ok(());
        }

        // This is an SCD diagnosis, mark it
        scd_result.add_scd_diagnosis(diagnosis.clone(), category as u8, is_congenital);
    }

    Ok(())
}

/// Categorize a diagnosis based on ICD-10 code
/// Returns (category, `is_congenital`, severity) if it's an SCD diagnosis, None otherwise
fn categorize_diagnosis(diagnosis_code: &str) -> Option<(ScdCategory, bool, SeverityLevel)> {
    // Clean and normalize the diagnosis code
    let clean_code = diagnosis_code.trim().to_uppercase();

    // Check if empty
    if clean_code.is_empty() {
        return None;
    }

    // Check for congenital malformations (Q codes)
    // Variable is used implicitly in pattern matching below
    #[allow(unused_variables)]
    let is_congenital = clean_code.starts_with('Q');

    // Special handling for specific codes that require additional parsing (4-character codes)
    if clean_code.len() >= 4 {
        let diag4 = &clean_code[..4];

        // Specific 4-character codes from the R reference implementation
        match diag4 {
            // Aplastic anemias
            "D610" | "D613" | "D618" | "D619" => {
                return Some((ScdCategory::BloodDisorder, false, SeverityLevel::Moderate));
            }
            // Hemophagocytic syndrome
            "D762" => {
                return Some((ScdCategory::BloodDisorder, false, SeverityLevel::Moderate));
            }
            // Degenerative disorders of the nervous system
            "G310" | "G318" | "G319" => {
                return Some((
                    ScdCategory::NeurologicalDisorder,
                    false,
                    SeverityLevel::Moderate,
                ));
            }
            // Myasthenia
            "G702" => {
                return Some((
                    ScdCategory::NeurologicalDisorder,
                    false,
                    SeverityLevel::Moderate,
                ));
            }
            // Myopathies
            "G710" | "G711" | "G712" | "G713" | "G736" => {
                return Some((
                    ScdCategory::NeurologicalDisorder,
                    false,
                    SeverityLevel::Severe,
                ));
            }
            // Spastic conditions
            "G811" | "G821" | "G824" => {
                return Some((
                    ScdCategory::NeurologicalDisorder,
                    false,
                    SeverityLevel::Moderate,
                ));
            }
            // Hydrocephalus in neoplastic disease
            "G941" => {
                return Some((
                    ScdCategory::NeurologicalDisorder,
                    false,
                    SeverityLevel::Moderate,
                ));
            }
            // COPD specific code
            "J448" => {
                return Some((
                    ScdCategory::RespiratoryDisorder,
                    false,
                    SeverityLevel::Severe,
                ));
            }
            // Disturbances of cerebral status of newborn
            "P910" | "P911" | "P912" => {
                return Some((
                    ScdCategory::NeurologicalDisorder,
                    true,
                    SeverityLevel::Moderate,
                ));
            }
            // Disorders of muscle tone of newborns
            "P941" | "P942" | "P943" | "P944" | "P945" | "P946" | "P947" | "P948" | "P949" => {
                return Some((
                    ScdCategory::NeurologicalDisorder,
                    true,
                    SeverityLevel::Moderate,
                ));
            }
            // Congenital malformations
            "Q790" => {
                return Some((
                    ScdCategory::RespiratoryDisorder,
                    true,
                    SeverityLevel::Moderate,
                ));
            }
            "Q792" | "Q793" => {
                return Some((
                    ScdCategory::GastrointestinalDisorder,
                    true,
                    SeverityLevel::Moderate,
                ));
            }
            "Q860" => {
                return Some((
                    ScdCategory::CongenitalDisorder,
                    true,
                    SeverityLevel::Moderate,
                ));
            }
            _ => { /* Continue to main categorization */ }
        }
    }

    // Categorize by first characters
    match &clean_code[..1] {
        // C codes are cancer and are considered severe (C00-C99)
        "C" => Some((ScdCategory::BloodDisorder, false, SeverityLevel::Severe)),

        // D codes cover blood and immune disorders
        "D" => {
            match &clean_code[..3] {
                // Immune system disorders (D80-D89)
                "D80" | "D81" | "D82" | "D83" | "D84" | "D86" | "D89" => {
                    Some((ScdCategory::ImmuneDisorder, false, SeverityLevel::Moderate))
                }
                // Blood disorders (D55-D77)
                "D55" | "D56" | "D57" | "D58" | "D59" | "D60" | "D61" | "D64" | "D65" | "D66"
                | "D67" | "D68" | "D69" | "D70" | "D71" | "D72" | "D73" | "D76" => {
                    let severity = if clean_code.starts_with("D57") {
                        // Sickle cell disorders
                        SeverityLevel::Severe
                    } else {
                        SeverityLevel::Moderate
                    };
                    Some((ScdCategory::BloodDisorder, false, severity))
                }
                _ => None,
            }
        }

        // E codes cover endocrine disorders
        "E" => {
            match &clean_code[..3] {
                // Insulin-dependent diabetes mellitus
                "E10" => Some((
                    ScdCategory::EndocrineDisorder,
                    false,
                    SeverityLevel::Moderate,
                )),
                // Adrenogenital disorders
                "E25" => Some((
                    ScdCategory::EndocrineDisorder,
                    false,
                    SeverityLevel::Moderate,
                )),
                // Disorders of amino-acid metabolism
                "E70" | "E71" | "E72" | "E730" => Some((
                    ScdCategory::EndocrineDisorder,
                    false,
                    SeverityLevel::Moderate,
                )),
                // Disorders of metabolism and cystic fibrosis (E74-E84)
                "E74" | "E75" | "E76" | "E77" | "E78" | "E79" | "E80" | "E83" | "E84" => {
                    let severity = if clean_code.starts_with("E84") {
                        // Cystic fibrosis
                        SeverityLevel::Severe
                    } else {
                        SeverityLevel::Moderate
                    };
                    Some((ScdCategory::EndocrineDisorder, false, severity))
                }
                // Other endocrine disorders
                "E22" | "E23" | "E24" | "E26" | "E27" | "E31" | "E34" | "E85" | "E88" => Some((
                    ScdCategory::EndocrineDisorder,
                    false,
                    SeverityLevel::Moderate,
                )),
                _ => None,
            }
        }

        // F codes cover mental disorders (incl. autism)
        "F" => {
            if clean_code.starts_with("F84") {
                // Autism spectrum disorders
                Some((
                    ScdCategory::NeurologicalDisorder,
                    false,
                    SeverityLevel::Moderate,
                ))
            } else {
                None
            }
        }

        // G codes cover neurological disorders
        "G" => {
            match &clean_code[..3] {
                // Spinal muscular atrophy and related syndromes
                "G12" => Some((
                    ScdCategory::NeurologicalDisorder,
                    false,
                    SeverityLevel::Severe,
                )),
                // Demyelinating diseases
                "G37" => Some((
                    ScdCategory::NeurologicalDisorder,
                    false,
                    SeverityLevel::Moderate,
                )),
                // Epilepsy
                "G40" => Some((
                    ScdCategory::NeurologicalDisorder,
                    false,
                    SeverityLevel::Moderate,
                )),
                // Hereditary and idiopathic neuropathy
                "G60" => Some((
                    ScdCategory::NeurologicalDisorder,
                    false,
                    SeverityLevel::Moderate,
                )),
                // Cerebral palsy
                "G80" => Some((
                    ScdCategory::NeurologicalDisorder,
                    false,
                    SeverityLevel::Moderate,
                )),
                // Hydrocephalus
                "G91" => Some((
                    ScdCategory::NeurologicalDisorder,
                    false,
                    SeverityLevel::Moderate,
                )),
                // Other neurological disorders
                "G11" | "G13" | "G23" | "G24" | "G25" | "G31" | "G41" | "G70" | "G71" | "G72"
                | "G81" | "G82" => {
                    let severity = if clean_code.starts_with("G12") || // Motor neuron disease
                                     clean_code.starts_with("G71")
                    {
                        // Muscular dystrophy
                        SeverityLevel::Severe
                    } else {
                        SeverityLevel::Moderate
                    };
                    Some((ScdCategory::NeurologicalDisorder, false, severity))
                }
                _ => None,
            }
        }

        // I codes cover cardiovascular disorders
        "I" => {
            match &clean_code[..3] {
                // Hypertensive renal disease
                "I12" => Some((
                    ScdCategory::CardiovascularDisorder,
                    false,
                    SeverityLevel::Moderate,
                )),
                // Pulmonary heart disease
                "I27" => Some((
                    ScdCategory::CardiovascularDisorder,
                    false,
                    SeverityLevel::Moderate,
                )),
                // Heart diseases (I30-I52)
                "I30" | "I31" | "I32" | "I33" | "I34" | "I35" | "I36" | "I37" | "I38" | "I39"
                | "I40" | "I41" | "I42" | "I43" | "I44" | "I45" | "I46" | "I47" | "I48" | "I49"
                | "I50" | "I51" | "I52" => {
                    let severity = if clean_code.starts_with("I50") {
                        // Heart failure
                        SeverityLevel::Severe
                    } else {
                        SeverityLevel::Moderate
                    };
                    Some((ScdCategory::CardiovascularDisorder, false, severity))
                }
                // Other vascular disorders
                "I81" | "I82" | "I83" => Some((
                    ScdCategory::CardiovascularDisorder,
                    false,
                    SeverityLevel::Moderate,
                )),
                _ => None,
            }
        }

        // J codes cover respiratory disorders
        "J" => {
            match &clean_code[..3] {
                // COPD
                "J44" => Some((
                    ScdCategory::RespiratoryDisorder,
                    false,
                    SeverityLevel::Severe,
                )),
                // Other interstitial pulmonary disease
                "J84" => Some((
                    ScdCategory::RespiratoryDisorder,
                    false,
                    SeverityLevel::Moderate,
                )),
                // Other respiratory disorders
                "J41" | "J42" | "J43" | "J45" | "J47" | "J60" | "J61" | "J62" | "J63" | "J64"
                | "J65" | "J66" | "J67" | "J68" | "J69" | "J70" | "J96" => {
                    let severity = if clean_code.starts_with("J96") {
                        // Respiratory failure
                        SeverityLevel::Severe
                    } else if clean_code.starts_with("J45") {
                        // Asthma
                        SeverityLevel::Mild
                    } else {
                        SeverityLevel::Moderate
                    };
                    Some((ScdCategory::RespiratoryDisorder, false, severity))
                }
                _ => None,
            }
        }

        // K codes cover gastrointestinal disorders
        "K" => {
            match &clean_code[..3] {
                // Gastro-esophageal reflux disease
                "K21" => Some((
                    ScdCategory::GastrointestinalDisorder,
                    false,
                    SeverityLevel::Mild,
                )),
                // Crohn's disease and ulcerative colitis
                "K50" | "K51" => Some((
                    ScdCategory::GastrointestinalDisorder,
                    false,
                    SeverityLevel::Moderate,
                )),
                // Diseases of the liver (K70-K77)
                "K70" | "K71" | "K72" | "K73" | "K74" | "K75" | "K76" | "K77" => {
                    let severity = if clean_code.starts_with("K74") {
                        // Fibrosis/cirrhosis of liver
                        SeverityLevel::Severe
                    } else {
                        SeverityLevel::Moderate
                    };
                    Some((ScdCategory::GastrointestinalDisorder, false, severity))
                }
                // Intestinal malabsorption
                "K90" => Some((
                    ScdCategory::GastrointestinalDisorder,
                    false,
                    SeverityLevel::Moderate,
                )),
                // Other gastrointestinal disorders
                "K86" | "K87" => Some((
                    ScdCategory::GastrointestinalDisorder,
                    false,
                    SeverityLevel::Moderate,
                )),
                _ => None,
            }
        }

        // M codes cover musculoskeletal disorders
        "M" => {
            match &clean_code[..3] {
                // Systematic involvement of connective tissue (M30-M35)
                "M30" | "M31" | "M32" | "M33" | "M34" | "M35" => {
                    let severity = if clean_code.starts_with("M32") || // Systemic lupus erythematosus
                                     clean_code.starts_with("M34")
                    {
                        // Systemic sclerosis
                        SeverityLevel::Severe
                    } else {
                        SeverityLevel::Moderate
                    };
                    Some((ScdCategory::MusculoskeletalDisorder, false, severity))
                }
                // Other musculoskeletal disorders
                "M05" | "M06" | "M07" | "M08" | "M09" | "M40" | "M41" | "M42" | "M43" | "M45"
                | "M46" => Some((
                    ScdCategory::MusculoskeletalDisorder,
                    false,
                    SeverityLevel::Moderate,
                )),
                _ => None,
            }
        }

        // N codes cover renal disorders
        "N" => {
            match &clean_code[..3] {
                // Nephritic syndrome (N03-N05)
                "N03" | "N04" | "N05" => {
                    Some((ScdCategory::RenalDisorder, false, SeverityLevel::Moderate))
                }
                // Hereditary nephropathy
                "N07" => Some((ScdCategory::RenalDisorder, false, SeverityLevel::Moderate)),
                // Obstructive and reflux uropathy
                "N13" => Some((ScdCategory::RenalDisorder, false, SeverityLevel::Moderate)),
                // Chronic kidney disease (N18-N19, N25-N27)
                "N18" | "N19" | "N25" | "N26" | "N27" => {
                    let severity = if clean_code.starts_with("N18") || clean_code.starts_with("N19")
                    {
                        // Kidney failure
                        // Check for specific stages of CKD
                        if clean_code.len() > 3
                            && clean_code.chars().nth(3).unwrap().is_ascii_digit()
                        {
                            let stage = clean_code.chars().nth(3).unwrap().to_digit(10).unwrap();
                            if stage >= 4 {
                                // CKD stage 4-5
                                SeverityLevel::Severe
                            } else {
                                SeverityLevel::Moderate
                            }
                        } else {
                            SeverityLevel::Moderate
                        }
                    } else {
                        SeverityLevel::Moderate
                    };
                    Some((ScdCategory::RenalDisorder, false, severity))
                }
                // Other renal disorders
                "N01" | "N02" | "N06" | "N08" | "N11" | "N12" | "N14" | "N15" | "N16" | "N20"
                | "N21" | "N22" | "N23" | "N24" | "N28" | "N29" => {
                    Some((ScdCategory::RenalDisorder, false, SeverityLevel::Moderate))
                }
                _ => None,
            }
        }

        // P codes cover perinatal conditions
        "P" => {
            match &clean_code[..3] {
                // Chronic respiratory disease originating in the perinatal period
                "P27" => Some((
                    ScdCategory::RespiratoryDisorder,
                    true,
                    SeverityLevel::Moderate,
                )),
                // Kernicterus
                "P57" => Some((
                    ScdCategory::NeurologicalDisorder,
                    true,
                    SeverityLevel::Moderate,
                )),
                // Disturbances of cerebral status of newborn
                "P91" => Some((
                    ScdCategory::NeurologicalDisorder,
                    true,
                    SeverityLevel::Moderate,
                )),
                // Disorders of muscle tone of newborns
                "P94" => Some((
                    ScdCategory::NeurologicalDisorder,
                    true,
                    SeverityLevel::Moderate,
                )),
                _ => None,
            }
        }

        // Q codes cover congenital malformations
        "Q" => {
            match &clean_code[..3] {
                // Congenital malformations of the nervous system (Q01-Q07)
                "Q01" | "Q02" | "Q03" | "Q04" | "Q05" | "Q06" | "Q07" => Some((
                    ScdCategory::NeurologicalDisorder,
                    true,
                    SeverityLevel::Severe,
                )),
                // Congenital malformations of the circulatory system (Q20-Q26)
                "Q20" | "Q21" | "Q22" | "Q23" | "Q24" | "Q25" | "Q26" | "Q27" | "Q28" => Some((
                    ScdCategory::CardiovascularDisorder,
                    true,
                    SeverityLevel::Severe,
                )),
                // Congenital malformations of respiratory system (Q30-Q33)
                "Q30" | "Q31" | "Q32" | "Q33" => Some((
                    ScdCategory::RespiratoryDisorder,
                    true,
                    SeverityLevel::Moderate,
                )),
                // Other congenital malformations of respiratory system, cleft lip and cleft palate (Q34-Q37)
                "Q34" | "Q35" | "Q36" | "Q37" => Some((
                    ScdCategory::RespiratoryDisorder,
                    true,
                    SeverityLevel::Moderate,
                )),
                // Congenital malformations of upper alimentary tract and intestines (Q38-Q45)
                "Q38" | "Q39" | "Q40" | "Q41" | "Q42" | "Q43" | "Q44" | "Q45" => Some((
                    ScdCategory::GastrointestinalDisorder,
                    true,
                    SeverityLevel::Moderate,
                )),
                // Congenital malformations of the urinary system (Q60-Q64)
                "Q60" | "Q61" | "Q62" | "Q63" | "Q64" => {
                    Some((ScdCategory::RenalDisorder, true, SeverityLevel::Moderate))
                }
                // Musculoskeletal congenital
                "Q77" | "Q78" | "Q79" => Some((
                    ScdCategory::MusculoskeletalDisorder,
                    true,
                    SeverityLevel::Moderate,
                )),
                // Congenital malformation syndromes
                "Q87" => Some((
                    ScdCategory::CongenitalDisorder,
                    true,
                    SeverityLevel::Moderate,
                )),
                // Chromosomal abnormalities (Q90-Q99)
                "Q90" | "Q91" | "Q92" | "Q93" | "Q94" | "Q95" | "Q96" | "Q97" | "Q98" | "Q99" => {
                    Some((ScdCategory::CongenitalDisorder, true, SeverityLevel::Severe))
                }
                // Other congenital
                "Q80" | "Q81" | "Q82" | "Q83" | "Q84" | "Q85" | "Q86" | "Q88" | "Q89" => Some((
                    ScdCategory::CongenitalDisorder,
                    true,
                    SeverityLevel::Moderate,
                )),
                _ => None,
            }
        }

        // Not an SCD diagnosis
        _ => None,
    }
}

/// Get all individuals with SCD from the results
#[must_use]
pub fn get_individuals_with_scd(scd_results: &HashMap<String, ScdResult>) -> Vec<String> {
    scd_results
        .iter()
        .filter(|(_, result)| result.has_scd)
        .map(|(pnr, _)| pnr.clone())
        .collect()
}

/// Get individuals with a specific SCD category
#[must_use]
pub fn get_individuals_by_category(
    scd_results: &HashMap<String, ScdResult>,
    category: ScdCategory,
) -> Vec<String> {
    scd_results
        .iter()
        .filter(|(_, result)| result.has_scd && result.scd_categories.contains(&(category as u8)))
        .map(|(pnr, _)| pnr.clone())
        .collect()
}

/// Get individuals by severity level
#[must_use]
pub fn get_individuals_by_severity(
    scd_results: &HashMap<String, ScdResult>,
    min_severity: SeverityLevel,
) -> Vec<String> {
    scd_results
        .iter()
        .filter(|(_, result)| result.has_scd && result.max_severity >= min_severity as i32)
        .map(|(pnr, _)| pnr.clone())
        .collect()
}

/// Extension trait for `DiagnosisCollection`
pub trait DiagnosisCollectionExt {
    /// Get all individuals who have any diagnoses
    fn individuals_with_diagnoses(&self) -> Vec<String>;
}

impl DiagnosisCollectionExt for DiagnosisCollection {
    fn individuals_with_diagnoses(&self) -> Vec<String> {
        // We'll implement this when we update the DiagnosisCollection
        // For now, return a combination of individuals with and without SCD
        let mut individuals = self.individuals_with_scd();
        individuals.extend(self.individuals_without_scd());
        individuals
    }
}
