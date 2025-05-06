//! Severe Chronic Disease (SCD) algorithm implementation
//!
//! This module implements the Severe Chronic Disease (SCD) algorithm for
//! identifying patients with severe chronic diseases based on ICD-10 diagnosis codes.

use arrow::array::{Array, ArrayRef, BooleanArray, Date32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::error::{IdsError, Result};
use crate::model::icd10::diagnosis_pattern::{DiagnosisPattern, normalize_diagnosis_code};

/// SCD disease categories with their associated ICD-10 codes
pub struct ScdDiseaseCodes {
    // Map of disease category to patterns
    patterns: HashMap<String, Vec<DiagnosisPattern>>,
    // Cached flat set of all prefix codes for quick lookup
    all_codes_cache: HashSet<String>,
}

impl ScdDiseaseCodes {
    /// Create a new `ScdDiseaseCodes` instance with predefined disease categories and codes
    #[must_use] 
    pub fn new() -> Self {
        let mut codes = Self {
            patterns: HashMap::with_capacity(10), // Pre-allocate for the 10 categories
            all_codes_cache: HashSet::with_capacity(150), // Approximate total codes
        };
        
        // Initialize with basic prefix patterns
        codes.add_basic_patterns();
        
        // Add more detailed patterns from reference implementation
        codes.add_detailed_patterns();
        
        codes
    }
    
    /// Add a prefix pattern to the codes
    fn add_prefix_pattern(&mut self, category: &str, prefix: &str, description: &str) {
        let pattern = DiagnosisPattern::new_prefix(prefix, description);
        
        // Add to the cache for fast lookup
        self.all_codes_cache.insert(prefix.to_string());
        
        // Add to the patterns map
        self.patterns
            .entry(category.to_string())
            .or_default()
            .push(pattern);
    }
    
    /// Add a regex pattern to the codes
    fn add_regex_pattern(&mut self, category: &str, regex: &str, description: &str) {
        if let Ok(pattern) = DiagnosisPattern::new_regex(regex, description) {
            // Add to the patterns map
            self.patterns
                .entry(category.to_string())
                .or_default()
                .push(pattern);
                
            // Note: Regex patterns aren't added to the prefix cache
        }
    }
    
    /// Add the basic prefix patterns (from the original implementation)
    fn add_basic_patterns(&mut self) {
        // Blood Disorders
        for prefix in &[
            "D55", "D56", "D57", "D58", "D59", "D60", "D61",
            "D64", "D65", "D66", "D67", "D68", "D69", "D70", "D71", "D72", "D73",
            "D76"
        ] {
            self.add_prefix_pattern("blood_disorders", prefix, "Blood disorders");
        }
        
        // Immune System Disorders
        for prefix in &["D80", "D81", "D82", "D83", "D84", "D86", "D89"] {
            self.add_prefix_pattern("immune_system", prefix, "Immune system disorders");
        }
        
        // Endocrine Disorders
        for prefix in &[
            "E22", "E23", "E24", "E25", "E26", "E27", "E31", "E34",
            "E70", "E71", "E72", "E73", "E74", "E75", "E76", "E77", 
            "E78", "E79", "E80", "E83", "E84", "E85", "E88"
        ] {
            self.add_prefix_pattern("endocrine", prefix, "Endocrine disorders");
        }
        
        // Neurological Disorders
        for prefix in &[
            "F84", "G11", "G12", "G13", "G23", "G24", "G25", "G31", 
            "G40", "G41", "G70", "G71", "G72", "G80", "G81", "G82"
        ] {
            self.add_prefix_pattern("neurological", prefix, "Neurological disorders");
        }
        
        // Cardiovascular Disorders
        for prefix in &["I27", "I42", "I43", "I50", "I81", "I82", "I83"] {
            self.add_prefix_pattern("cardiovascular", prefix, "Cardiovascular disorders");
        }
        
        // Respiratory Disorders
        for prefix in &[
            "J41", "J42", "J43", "J44", "J45", "J47", "J60", "J61", "J62", 
            "J63", "J64", "J65", "J66", "J67", "J68", "J69", "J70", "J84", "J96"
        ] {
            self.add_prefix_pattern("respiratory", prefix, "Respiratory disorders");
        }
        
        // Gastrointestinal Disorders
        for prefix in &["K50", "K51", "K73", "K74", "K86", "K87", "K90"] {
            self.add_prefix_pattern("gastrointestinal", prefix, "Gastrointestinal disorders");
        }
        
        // Musculoskeletal Disorders
        for prefix in &[
            "M05", "M06", "M07", "M08", "M09", "M30", "M31", "M32", "M33",
            "M34", "M35", "M40", "M41", "M42", "M43", "M45", "M46"
        ] {
            self.add_prefix_pattern("musculoskeletal", prefix, "Musculoskeletal disorders");
        }
        
        // Renal Disorders
        for prefix in &[
            "N01", "N02", "N03", "N04", "N05", "N06", "N07", "N08", 
            "N11", "N12", "N13", "N14", "N15", "N16", "N18", "N19", 
            "N20", "N21", "N22", "N23", "N24", "N25", "N26", "N27", "N28", "N29"
        ] {
            self.add_prefix_pattern("renal", prefix, "Renal disorders");
        }
        
        // Congenital Disorders
        for prefix in &[
            "P27", "Q01", "Q02", "Q03", "Q04", "Q05", "Q06", "Q07", 
            "Q20", "Q21", "Q22", "Q23", "Q24", "Q25", "Q26", "Q27", "Q28",
            "Q30", "Q31", "Q32", "Q33", "Q34", "Q35", "Q36", "Q37", 
            "Q38", "Q39", "Q40", "Q41", "Q42", "Q43", "Q44", "Q45", 
            "Q60", "Q61", "Q62", "Q63", "Q64", "Q77", "Q78", "Q79", 
            "Q80", "Q81", "Q82", "Q83", "Q84", "Q85", "Q86", "Q87", "Q89"
        ] {
            self.add_prefix_pattern("congenital", prefix, "Congenital disorders");
        }
    }
    
    /// Add detailed patterns from the reference implementation
    fn add_detailed_patterns(&mut self) {
        // Blood disorders - specific regex patterns
        self.add_regex_pattern("blood_disorders", "D61[0389]", "Aplastic anaemias");
        self.add_regex_pattern("blood_disorders", "D762", "Haemophagocytic syndrome");
        
        // Immune system - specific patterns 
        self.add_regex_pattern("immune_system", "D8[012]", "Immunodeficiencies");
        
        // Neurological disorders - specific patterns
        self.add_regex_pattern("neurological", "G11[01234789]", "Hereditary ataxias");
        self.add_regex_pattern("neurological", "G12[012345]", "Motor neuron diseases");
        self.add_regex_pattern("neurological", "G23[123]", "Other degenerative diseases of basal ganglia");
        self.add_regex_pattern("neurological", "G24[01345]", "Dystonia");
        self.add_regex_pattern("neurological", "G40[0123456789]", "Epilepsy");
        self.add_regex_pattern("neurological", "G41[0129]", "Status epilepticus");
        
        // Respiratory disorders - specific patterns
        self.add_regex_pattern("respiratory", "J43[012]", "Emphysema");
        self.add_regex_pattern("respiratory", "J44[019]", "COPD");
        self.add_regex_pattern("respiratory", "J47", "Bronchiectasis");
        self.add_regex_pattern("respiratory", "J84[189]", "Other interstitial pulmonary diseases");
        
        // Gastrointestinal disorders - specific patterns
        self.add_regex_pattern("gastrointestinal", "K50[012345678]", "Crohn's disease");
        self.add_regex_pattern("gastrointestinal", "K51[012345678]", "Ulcerative colitis");
        self.add_regex_pattern("gastrointestinal", "K74[0123456]", "Hepatic fibrosis and cirrhosis");
        
        // Musculoskeletal disorders - specific patterns
        self.add_regex_pattern("musculoskeletal", "M05[0123489]", "Rheumatoid arthritis with other organ involvement");
        self.add_regex_pattern("musculoskeletal", "M06[0123489]", "Other rheumatoid arthritis");
        self.add_regex_pattern("musculoskeletal", "M32[18]", "Systemic lupus erythematosus");
        self.add_regex_pattern("musculoskeletal", "M33[012]", "Dermatopolymyositis");
        self.add_regex_pattern("musculoskeletal", "M34[0189]", "Systemic sclerosis");
        
        // Renal disorders - specific patterns
        self.add_regex_pattern("renal", "N18[12345]", "Chronic kidney disease");
    }
    
    /// Get all SCD codes as a flat set (returns a reference to pre-computed set)
    #[must_use] 
    pub const fn all_codes(&self) -> &HashSet<String> {
        &self.all_codes_cache
    }
    
    /// Check if a diagnosis code is a SCD code with caching for better performance
    #[must_use] 
    pub fn is_scd_code(&self, diagnosis: &str) -> bool {
        use std::sync::Mutex;
        use once_cell::sync::Lazy;
        
        // Cache of previously seen diagnosis codes and their SCD status
        // This avoids repeated normalization and pattern matching for common codes
        static DIAGNOSIS_CACHE: Lazy<Mutex<HashMap<String, bool>>> = 
            Lazy::new(|| Mutex::new(HashMap::with_capacity(10000)));
        
        // Check cache first for this exact diagnosis string
        {
            let cache = DIAGNOSIS_CACHE.lock().unwrap();
            if let Some(&result) = cache.get(diagnosis) {
                return result;
            }
        }
        
        // If not in cache, normalize the code
        let normalized = match normalize_diagnosis_code(diagnosis) {
            Some(norm) => norm,
            None => {
                // Cache negative result
                let mut cache = DIAGNOSIS_CACHE.lock().unwrap();
                if cache.len() < 100000 { // Limit cache size
                    cache.insert(diagnosis.to_string(), false);
                }
                return false;
            },
        };
        
        // First check if it's a simple prefix match (faster)
        for prefix in &self.all_codes_cache {
            if normalized.full_code.starts_with(prefix) {
                // Cache positive result
                let mut cache = DIAGNOSIS_CACHE.lock().unwrap();
                if cache.len() < 100000 { // Limit cache size
                    cache.insert(diagnosis.to_string(), true);
                }
                return true;
            }
        }
        
        // If not found in prefix cache, check regex patterns
        let mut result = false;
        for patterns in self.patterns.values() {
            for pattern in patterns {
                if pattern.matches(&normalized) {
                    result = true;
                    break;
                }
            }
            if result {
                break;
            }
        }
        
        // Cache the result before returning
        let mut cache = DIAGNOSIS_CACHE.lock().unwrap();
        if cache.len() < 100000 { // Limit cache size
            cache.insert(diagnosis.to_string(), result);
        }
        
        result
    }
    
    /// Get the disease categories for a diagnosis code with caching for better performance
    #[must_use] 
    pub fn get_disease_categories(&self, diagnosis: &str) -> HashSet<String> {
        use std::sync::Mutex;
        use once_cell::sync::Lazy;
        
        // Cache of previously seen diagnosis codes and their disease categories
        static CATEGORY_CACHE: Lazy<Mutex<HashMap<String, HashSet<String>>>> = 
            Lazy::new(|| Mutex::new(HashMap::with_capacity(10000)));
        
        // Check cache first for this exact diagnosis string
        {
            let cache = CATEGORY_CACHE.lock().unwrap();
            if let Some(cached_categories) = cache.get(diagnosis) {
                return cached_categories.clone();
            }
        }
        
        let mut categories = HashSet::new();
        
        // First normalize the code
        let normalized = match normalize_diagnosis_code(diagnosis) {
            Some(norm) => norm,
            None => {
                // Cache empty result
                let mut cache = CATEGORY_CACHE.lock().unwrap();
                if cache.len() < 100000 { // Limit cache size
                    cache.insert(diagnosis.to_string(), categories.clone());
                }
                return categories;
            },
        };
        
        // Check each category's patterns
        for (category, patterns) in &self.patterns {
            for pattern in patterns {
                if pattern.matches(&normalized) {
                    categories.insert(category.clone());
                    break; // No need to check other patterns in this category
                }
            }
        }
        
        // Cache the result before returning
        let mut cache = CATEGORY_CACHE.lock().unwrap();
        if cache.len() < 100000 { // Limit cache size
            cache.insert(diagnosis.to_string(), categories.clone());
        }
        
        categories
    }
    
    /// Get all available disease categories
    #[must_use] 
    pub fn get_all_categories(&self) -> Vec<String> {
        self.patterns.keys().cloned().collect()
    }
}

impl Default for ScdDiseaseCodes {
    fn default() -> Self {
        Self::new()
    }
}

/// Configuration for SCD algorithm
pub struct ScdConfig {
    /// Diagnosis columns to check for SCD codes
    pub diagnosis_columns: Vec<String>,
    /// Column containing the date of diagnosis
    pub date_column: String,
    /// Column containing the patient ID
    pub patient_id_column: String,
}

impl Default for ScdConfig {
    fn default() -> Self {
        Self {
            diagnosis_columns: vec!["primary_diagnosis".to_string()],
            date_column: "diagnosis_date".to_string(),
            patient_id_column: "patient_id".to_string(),
        }
    }
}

/// Result of SCD algorithm for a single patient
#[derive(Debug, Clone)]
pub struct ScdResult {
    /// Patient identifier
    pub patient_id: String,
    /// Whether the patient has any SCD
    pub is_scd: bool,
    /// First date when SCD was diagnosed
    pub first_scd_date: Option<NaiveDate>,
    /// Disease categories for this patient
    pub disease_categories: HashMap<String, bool>,
}

/// Apply the SCD algorithm to health data with parallel processing
pub fn apply_scd_algorithm(
    health_data: &RecordBatch,
    config: &ScdConfig,
) -> Result<Vec<ScdResult>> {
    use rayon::prelude::*;
    use std::sync::{Arc, Mutex};
    
    // Initialize SCD disease codes (shared across threads)
    let scd_codes = Arc::new(ScdDiseaseCodes::new());
    let all_categories = scd_codes.get_all_categories();
    
    // Get required columns for the algorithm
    let patient_id_col = health_data
        .column_by_name(&config.patient_id_column)
        .ok_or_else(|| {
            IdsError::Data(format!(
                "Patient ID column '{}' not found in health data",
                config.patient_id_column
            ))
        })?;
    
    // Create a map to store results by patient ID
    let patient_id_array = patient_id_col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
        IdsError::Data(format!(
            "Patient ID column '{}' is not a string array",
            config.patient_id_column
        ))
    })?;
    
    // Initialize patient data with estimated capacity
    let num_rows = health_data.num_rows();
    let estimated_patients = (num_rows as f64 * 0.2).max(100.0) as usize; // Heuristic
    
    // Store temporary per-patient data during processing
    #[derive(Default)]
    struct PatientData {
        is_scd: bool,
        first_scd_date: Option<NaiveDate>,
        categories: HashMap<String, bool>,
    }
    
    // Create a thread-safe map for patient data
    let patient_data: Arc<Mutex<HashMap<String, PatientData>>> = 
        Arc::new(Mutex::new(HashMap::with_capacity(estimated_patients)));
    
    // Extract and validate date column
    let date_col_opt = health_data.column_by_name(&config.date_column);
    let date_array_opt = if let Some(date_col) = date_col_opt {
        date_col.as_any().downcast_ref::<Date32Array>().map(Arc::new)
    } else {
        None
    };
    
    // Extract diagnosis columns for parallel processing
    let mut diag_arrays = Vec::with_capacity(config.diagnosis_columns.len());
    for diag_col_name in &config.diagnosis_columns {
        if let Some(diag_col) = health_data.column_by_name(diag_col_name) {
            if let Some(diag_array) = diag_col.as_any().downcast_ref::<StringArray>() {
                diag_arrays.push(Arc::new(diag_array));
            }
        }
    }
    
    // Process records in configurable chunk sizes for better cache efficiency
    const CHUNK_SIZE: usize = 10000;
    let chunks = (0..num_rows).collect::<Vec<usize>>()
        .chunks(CHUNK_SIZE)
        .map(|chunk| chunk.to_vec())
        .collect::<Vec<Vec<usize>>>();
    
    // Process chunks in parallel
    chunks.into_par_iter().for_each(|chunk| {
        // Create thread-local maps to reduce lock contention
        let mut thread_patient_data: HashMap<String, PatientData> = HashMap::new();
        
        // Process each record in this chunk
        for &row_idx in &chunk {
            // Skip records with null patient ID
            if patient_id_array.is_null(row_idx) {
                continue;
            }
            
            // Get patient ID (we'll clone only when needed)
            let patient_id = patient_id_array.value(row_idx);
            
            // Check if we've already seen this patient in current thread
            if !thread_patient_data.contains_key(patient_id) {
                // Check if this patient is already in the shared map
                let mut global_map = patient_data.lock().unwrap();
                if !global_map.contains_key(patient_id) {
                    // Initialize categories map
                    let mut categories = HashMap::with_capacity(all_categories.len());
                    for category in &all_categories {
                        categories.insert(category.clone(), false);
                    }
                    
                    // Insert into global map
                    global_map.insert(patient_id.to_string(), PatientData {
                        is_scd: false,
                        first_scd_date: None,
                        categories,
                    });
                }
                
                // Copy to thread-local map
                if let Some(patient) = global_map.get(patient_id) {
                    thread_patient_data.insert(
                        patient_id.to_string(), 
                        PatientData {
                            is_scd: patient.is_scd,
                            first_scd_date: patient.first_scd_date,
                            categories: patient.categories.clone(),
                        }
                    );
                }
            }
            
            // Get patient data from thread-local map
            let patient = thread_patient_data.get_mut(patient_id).unwrap();
            
            // Get the diagnosis date (may be null)
            let diagnosis_date = if let Some(date_array) = &date_array_opt {
                if date_array.is_null(row_idx) {
                    None
                } else {
                    // Convert from days since epoch to NaiveDate
                    let days_since_epoch = date_array.value(row_idx);
                    Some(
                        NaiveDate::from_ymd_opt(1970, 1, 1)
                            .unwrap()
                            .checked_add_days(chrono::Days::new(days_since_epoch as u64))
                            .unwrap(),
                    )
                }
            } else {
                None
            };
            
            // Check each diagnosis column
            let mut found_scd = false;
            let mut scd_categories = HashSet::new();
            
            for diag_array in &diag_arrays {
                if diag_array.is_null(row_idx) {
                    continue;
                }
                
                // Extract diagnosis code (without cloning)
                let diagnosis = diag_array.value(row_idx);
                
                // Check if it's a SCD code
                if scd_codes.is_scd_code(diagnosis) {
                    found_scd = true;
                    
                    // Get categories for this diagnosis
                    let categories = scd_codes.get_disease_categories(diagnosis);
                    scd_categories.extend(categories);
                }
            }
            
            // Update patient data if SCD was found
            if found_scd {
                patient.is_scd = true;
                
                // Update first diagnosis date
                if let Some(date) = diagnosis_date {
                    if let Some(current_date) = patient.first_scd_date {
                        if date < current_date {
                            patient.first_scd_date = Some(date);
                        }
                    } else {
                        patient.first_scd_date = Some(date);
                    }
                }
                
                // Update disease categories
                for category in scd_categories {
                    if let Some(has_category) = patient.categories.get_mut(&category) {
                        *has_category = true;
                    }
                }
            }
        }
        
        // Merge thread-local data back into shared map
        let mut global_map = patient_data.lock().unwrap();
        for (id, thread_patient) in thread_patient_data {
            if let Some(global_patient) = global_map.get_mut(&id) {
                // Only update if thread data has SCD
                if thread_patient.is_scd {
                    global_patient.is_scd = true;
                    
                    // Update first diagnosis date if needed
                    if let Some(thread_date) = thread_patient.first_scd_date {
                        if let Some(global_date) = global_patient.first_scd_date {
                            if thread_date < global_date {
                                global_patient.first_scd_date = Some(thread_date);
                            }
                        } else {
                            global_patient.first_scd_date = Some(thread_date);
                        }
                    }
                    
                    // Merge categories
                    for (category, &has_category) in &thread_patient.categories {
                        if has_category {
                            if let Some(global_has_category) = global_patient.categories.get_mut(category) {
                                *global_has_category = true;
                            }
                        }
                    }
                }
            }
        }
    });
    
    // Convert the patient data map to a vector of ScdResult objects
    let results = patient_data
        .lock()
        .unwrap()
        .iter()
        .map(|(patient_id, data)| ScdResult {
            patient_id: patient_id.clone(),
            is_scd: data.is_scd,
            first_scd_date: data.first_scd_date,
            disease_categories: data.categories.clone(),
        })
        .collect();
    
    Ok(results)
}

/// Convert SCD results to a `RecordBatch`
pub fn scd_results_to_record_batch(results: &[ScdResult]) -> Result<RecordBatch> {
    if results.is_empty() {
        return Err(IdsError::Data("No SCD results to convert".to_string()));
    }
    
    // Get all unique categories
    let mut all_categories = HashSet::new();
    for result in results {
        all_categories.extend(result.disease_categories.keys().cloned());
    }
    let categories: Vec<String> = all_categories.into_iter().collect();
    
    // Prepare data for the batch
    let mut patient_ids = Vec::with_capacity(results.len());
    let mut is_scd_values = Vec::with_capacity(results.len());
    let mut first_scd_dates = Vec::with_capacity(results.len());
    
    // One vector per category
    let mut category_values: HashMap<String, Vec<Option<bool>>> = HashMap::new();
    for category in &categories {
        category_values.insert(category.clone(), Vec::with_capacity(results.len()));
    }
    
    // Populate data
    for result in results {
        patient_ids.push(Some(result.patient_id.clone()));
        is_scd_values.push(Some(result.is_scd));
        
        // Convert date to i32 days since epoch
        if let Some(date) = result.first_scd_date {
            let days = date
                .signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                .num_days() as i32;
            first_scd_dates.push(Some(days));
        } else {
            first_scd_dates.push(None);
        }
        
        // Add category values
        for category in &categories {
            if let Some(&has_category) = result.disease_categories.get(category) {
                category_values.get_mut(category).unwrap().push(Some(has_category));
            } else {
                category_values.get_mut(category).unwrap().push(None);
            }
        }
    }
    
    // Create arrays
    let patient_id_array = StringArray::from(patient_ids);
    let is_scd_array = BooleanArray::from(is_scd_values);
    let first_scd_date_array = Date32Array::from(first_scd_dates);
    
    // Create field list and column list
    let mut fields = vec![
        Field::new("patient_id", DataType::Utf8, false),
        Field::new("is_scd", DataType::Boolean, true),
        Field::new("first_scd_date", DataType::Date32, true),
    ];
    
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(patient_id_array) as ArrayRef,
        Arc::new(is_scd_array) as ArrayRef,
        Arc::new(first_scd_date_array) as ArrayRef,
    ];
    
    // Add category columns
    for category in &categories {
        let field_name = format!("category_{category}");
        fields.push(Field::new(&field_name, DataType::Boolean, true));
        
        let category_array = BooleanArray::from(category_values.get(category).unwrap().clone());
        columns.push(Arc::new(category_array) as ArrayRef);
    }
    
    // Create schema and batch
    let schema = Schema::new(fields);
    let batch = RecordBatch::try_new(Arc::new(schema), columns)
        .map_err(|e| IdsError::Data(format!("Failed to create SCD result batch: {e}")))?;
    
    Ok(batch)
}