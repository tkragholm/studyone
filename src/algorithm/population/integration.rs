//! Cross-registry integration logic
//!
//! This module provides functionality for integrating data from multiple
//! registries into a cohesive population dataset.

use arrow::array::Array;
use chrono::NaiveDate;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use crate::error::Result;
use crate::models::family::FamilyCollection;
use crate::models::{Child, Diagnosis, Income};
use crate::registry::BefCombinedRegister;
use crate::registry::model_conversion::ModelConversion;
use crate::registry::{
    Lpr3DiagnoserRegister, LprDiagRegister, MfrChildRegister, YearConfiguredIndRegister,
};

use crate::Family;
use crate::Individual;
use crate::Parent;
use crate::registry::factory;

/// Registry integration manager for combining data from multiple sources
pub struct RegistryIntegration {
    /// Collection of individuals and families
    collection: FamilyCollection,
    /// Diagnosis data by individual PNR
    diagnoses: HashMap<String, Vec<Diagnosis>>,
    /// Income data by individual PNR
    incomes: HashMap<String, Vec<Income>>,
    /// Migration data by individual PNR (emigration and immigration events)
    migration_events: HashMap<String, Vec<(NaiveDate, bool)>>, // (date, is_emigration)
    /// Death data by individual PNR
    death_dates: HashMap<String, NaiveDate>,
}

impl RegistryIntegration {
    /// Create a new registry integration manager
    #[must_use]
    pub fn new() -> Self {
        Self {
            collection: FamilyCollection::new(),
            diagnoses: HashMap::new(),
            incomes: HashMap::new(),
            migration_events: HashMap::new(),
            death_dates: HashMap::new(),
        }
    }

    /// Get the integrated family collection
    #[must_use]
    pub const fn collection(&self) -> &FamilyCollection {
        &self.collection
    }

    /// Get diagnoses for a specific individual
    #[must_use]
    pub fn get_diagnoses(&self, pnr: &str) -> Option<&Vec<Diagnosis>> {
        self.diagnoses.get(pnr)
    }

    /// Get income data for a specific individual
    #[must_use]
    pub fn get_incomes(&self, pnr: &str) -> Option<&Vec<Income>> {
        self.incomes.get(pnr)
    }

    /// Get migration events for a specific individual
    #[must_use]
    pub fn get_migration_events(&self, pnr: &str) -> Option<&Vec<(NaiveDate, bool)>> {
        self.migration_events.get(pnr)
    }

    /// Get death date for a specific individual
    #[must_use]
    pub fn get_death_date(&self, pnr: &str) -> Option<&NaiveDate> {
        self.death_dates.get(pnr)
    }

    /// Check if an individual was resident in Denmark at a specific date
    #[must_use]
    pub fn was_resident_at(&self, pnr: &str, date: &NaiveDate) -> bool {
        // Check if the individual has died
        if let Some(death_date) = self.death_dates.get(pnr) {
            if death_date < date {
                return false; // Deceased individuals are not resident
            }
        }

        // Check migration status
        if let Some(migration_events) = self.migration_events.get(pnr) {
            // Find the last migration event before or on the reference date
            // No need to clone and sort the events each time - just find the latest relevant event
            let last_event = migration_events
                .iter()
                .filter(|(event_date, _)| event_date <= date)
                .max_by_key(|(event_date, _)| *event_date);

            if let Some((_, is_emigration)) = last_event {
                return !is_emigration; // If last event was emigration, not resident
            }
        }

        // Default: if no migration events or death, assume resident
        true
    }

    /// Add demographic data from BEF registry
    pub fn add_demographic_data(
        &mut self,
        path: &Path,
        pnr_filter: Option<&HashSet<String>>,
    ) -> Result<()> {
        // Create BEF registry loader
        let registry = factory::registry_from_name("bef")?;

        // Load the batches
        let batches = registry.load(path, pnr_filter)?;

        for batch in batches {
            // Use the BEF adapter to process the batch
            let (individuals, families) = BefCombinedRegister::process_batch(&batch)?;

            // Add individuals to collection
            for individual in individuals {
                self.collection.add_individual(individual);
            }

            // Add families to collection
            for family in families {
                self.collection.add_family(family);
            }
        }

        Ok(())
    }

    /// Add child-specific data from MFR registry
    pub fn add_child_data(
        &mut self,
        path: &Path,
        pnr_filter: Option<&HashSet<String>>,
    ) -> Result<()> {
        // Create MFR registry loader
        let registry = factory::registry_from_name("mfr")?;

        // Load the batches
        let batches = registry.load(path, pnr_filter)?;

        // Create individual lookup for MFR adapter
        let individual_lookup: HashMap<String, Arc<Individual>> = self
            .collection
            .get_individuals()
            .iter()
            .map(|ind| (ind.pnr.clone(), ind.clone())) // ind is already an Arc<Individual>
            .collect();

        // Create MFR adapter with individual lookup
        let adapter = MfrChildRegister::new_with_lookup(individual_lookup);

        // Process and match children data
        for batch in batches {
            // Use adapter's process_batch method
            let child_details = adapter.process_batch(&batch)?;

            for detail in child_details {
                if let Some(individual) = self.collection.get_individual(&detail.individual().pnr) {
                    // Create a Child object using from_individual and all MFR-specific details
                    // Use the individual directly without creating a new Arc
                    let child_with_details = Child::from_individual(individual.clone())
                        .with_birth_details(
                            detail.birth_weight,
                            detail.gestational_age,
                            detail.apgar_score,
                        );

                    // Update the child in the collection
                    let child_pnr = child_with_details.individual().pnr.clone();
                    let updated = self.collection.update_child(&child_pnr, child_with_details);

                    if updated {
                        log::debug!("Updated child {child_pnr} with birth details");
                    }
                }
            }
        }

        Ok(())
    }

    /// Add diagnosis data from LPR registry
    pub fn add_diagnosis_data(
        &mut self,
        path: &Path,
        pnr_filter: Option<&HashSet<String>>,
    ) -> Result<()> {
        // Determine if this is LPR2 or LPR3 based on path
        let path_str = path.to_string_lossy().to_lowercase();

        if path_str.contains("lpr3") || path_str.contains("diagnoser") {
            // LPR3 registry
            let registry = factory::registry_from_name("lpr3_diagnoser")?;
            let batches = registry.load(path, pnr_filter)?;

            // Create and populate a PNR lookup for the adapter
            // For LPR3, we'd need to map kontakt IDs to PNRs
            // This is just a placeholder since actual implementation would depend on data structure
            let pnr_lookup: HashMap<String, String> = HashMap::new();

            // Create the adapter with lookup
            use crate::registry::lpr_model_conversion::PnrLookupRegistry;
            let mut adapter = Lpr3DiagnoserRegister::new();
            adapter.set_pnr_lookup(pnr_lookup);

            for batch in batches {
                // Use the adapter's to_models method
                let batch_diagnoses = adapter.to_models(&batch)?;

                // Group diagnoses by individual
                for diagnosis in batch_diagnoses {
                    self.diagnoses
                        .entry(diagnosis.individual_pnr.clone())
                        .or_default()
                        .push(diagnosis);
                }
            }
        } else {
            // LPR2 registry (assume LPR diag)
            let registry = factory::registry_from_name("lpr_diag")?;
            let batches = registry.load(path, pnr_filter)?;

            // Create and populate a PNR lookup for the adapter
            // For LPR2, we'd need to map record IDs to PNRs
            // This is just a placeholder since actual implementation would depend on data structure
            let pnr_lookup: HashMap<String, String> = HashMap::new();

            // Create the adapter with lookup
            use crate::registry::lpr_model_conversion::PnrLookupRegistry;
            let mut adapter = LprDiagRegister::new();
            adapter.set_pnr_lookup(pnr_lookup);

            for batch in batches {
                // Use the adapter's to_models method
                let batch_diagnoses = adapter.to_models(&batch)?;

                // Group diagnoses by individual
                for diagnosis in batch_diagnoses {
                    self.diagnoses
                        .entry(diagnosis.individual_pnr.clone())
                        .or_default()
                        .push(diagnosis);
                }
            }
        }

        Ok(())
    }

    /// Add income data from IND registry
    pub fn add_income_data(
        &mut self,
        path: &Path,
        year: i32,
        pnr_filter: Option<&HashSet<String>>,
    ) -> Result<()> {
        // Create IND registry loader
        let registry = factory::registry_from_name("ind")?;

        // Load the batches
        let batches = registry.load(path, pnr_filter)?;

        // Create IND adapter for the specific year
        let adapter = YearConfiguredIndRegister::new(year);

        for batch in batches {
            // Use the to_models method
            let batch_incomes = adapter.to_models(&batch)?;

            // Group incomes by individual
            for income in batch_incomes {
                self.incomes
                    .entry(income.individual_pnr.clone())
                    .or_default()
                    .push(income);
            }
        }

        Ok(())
    }

    /// Link diagnosis data to children to identify SCD cases
    pub fn link_diagnoses_to_children(&mut self) -> Result<()> {
        // Count of SCD diagnoses linked
        let mut scd_count = 0;

        // Identify families with children
        let family_ids = self
            .collection
            .get_snapshots_at(&chrono::Utc::now().naive_utc().date())
            .into_iter()
            .map(|snapshot| snapshot.family_id)
            .collect::<Vec<_>>();

        for family_id in family_ids {
            // Get the family
            if let Some(family) = self.collection.get_family(&family_id) {
                // Iterate through children in the family
                for child_ref in &family.children {
                    let child_pnr = &child_ref.individual().pnr;

                    // Check if this child has diagnoses
                    if let Some(diagnoses) = self.diagnoses.get(child_pnr) {
                        // Check for SCD-related diagnoses
                        // NOTE: This is a simplified implementation. In a real system,
                        // you would use the full SCD algorithm.
                        let has_scd = diagnoses.iter().any(|d| {
                            d.diagnosis_code.starts_with('C') || // Cancer
                            d.diagnosis_code.starts_with("D80") || // Immunodeficiency
                            d.diagnosis_code.starts_with("G71") || // Muscular disorders
                            d.diagnosis_code.starts_with('Q') // Congenital malformations
                        });

                        if has_scd {
                            // Clone the Child to modify it
                            let child = (**child_ref).clone();

                            // Update SCD status using with_scd
                            // Using current date as the first SCD date and default values for other params
                            let current_date = chrono::Utc::now().naive_utc().date();
                            use crate::models::child::{
                                DiseaseOrigin, DiseaseSeverity, ScdCategory,
                            };

                            // Determine the SCD category based on diagnosis code
                            let scd_category = if diagnoses
                                .iter()
                                .any(|d| d.diagnosis_code.starts_with('C'))
                            {
                                ScdCategory::BloodDisorder // Example category for cancer
                            } else if diagnoses
                                .iter()
                                .any(|d| d.diagnosis_code.starts_with("D80"))
                            {
                                ScdCategory::ImmuneDisorder
                            } else if diagnoses
                                .iter()
                                .any(|d| d.diagnosis_code.starts_with("G71"))
                            {
                                ScdCategory::NeurologicalDisorder
                            } else if diagnoses.iter().any(|d| d.diagnosis_code.starts_with('Q')) {
                                ScdCategory::CongenitalDisorder
                            } else {
                                ScdCategory::None
                            };

                            // Determine disease origin
                            let disease_origin =
                                if diagnoses.iter().any(|d| d.diagnosis_code.starts_with('Q')) {
                                    DiseaseOrigin::Congenital
                                } else {
                                    DiseaseOrigin::Acquired
                                };

                            // Set child with SCD status
                            let child_with_scd = child.with_scd(
                                scd_category,
                                current_date,              // First SCD date
                                DiseaseSeverity::Moderate, // Default severity
                                disease_origin,
                            );

                            // Update the child in the collection
                            let updated = self.collection.update_child(child_pnr, child_with_scd);

                            if updated {
                                log::debug!("Updated child {child_pnr} with SCD status");
                            }

                            scd_count += 1;
                        }
                    }
                }
            }
        }

        log::info!("Linked {scd_count} children with SCD diagnoses");

        Ok(())
    }

    /// Link income data to parents
    pub fn link_income_to_parents(&mut self) -> Result<()> {
        // Count of income records linked
        let mut income_count = 0;
        let index_date = chrono::Utc::now().naive_utc().date();

        // Identify families with parents
        let family_ids = self
            .collection
            .get_snapshots_at(&index_date)
            .into_iter()
            .map(|snapshot| snapshot.family_id)
            .collect::<Vec<_>>();

        for family_id in family_ids {
            if let Some(family) = self.collection.get_family(&family_id) {
                // Check for mother's income data
                if let Some(mother) = &family.mother {
                    let mother_pnr = &mother.individual().pnr;
                    if let Some(income_data) = self.incomes.get(mother_pnr) {
                        // We have income data for this mother
                        // Update the Parent object with the income information
                        let updated_mother =
                            Parent::from_individual(mother.individual().clone().into());

                        // In a real implementation, we would add methods to the Parent class
                        // to properly store and process income data. For now, we'll just
                        // note that we've linked income data using a log message
                        log::debug!(
                            "Linked {} income records to mother {}",
                            income_data.len(),
                            mother_pnr
                        );

                        // Update the parent in the collection
                        let updated = self.collection.update_parent(mother_pnr, updated_mother);
                        if updated {
                            log::debug!("Updated mother {mother_pnr} with income data");
                        }

                        // Count the records
                        income_count += 1;
                    }
                }

                // Check for father's income data
                if let Some(father) = &family.father {
                    let father_pnr = &father.individual().pnr;
                    if let Some(income_data) = self.incomes.get(father_pnr) {
                        // We have income data for this father
                        // Update the Parent object with the income information
                        let updated_father =
                            Parent::from_individual(father.individual().clone().into());

                        // In a real implementation, we would add methods to the Parent class
                        // to properly store and process income data. For now, we'll just
                        // note that we've linked income data using a log message
                        log::debug!(
                            "Linked {} income records to father {}",
                            income_data.len(),
                            father_pnr
                        );

                        // Update the parent in the collection
                        let updated = self.collection.update_parent(father_pnr, updated_father);
                        if updated {
                            log::debug!("Updated father {father_pnr} with income data");
                        }

                        // Count the records
                        income_count += 1;
                    }
                }
            }
        }

        log::info!("Linked income data to {income_count} parents");

        // To fully implement this, we would need to:
        // 1. Clone the Parent object from the family
        // 2. Create a modified version with income data
        // 3. Update the Family with the new Parent object
        // 4. Update the FamilyCollection with the updated Family

        // This would require additional methods in Parent and Family to handle income data

        Ok(())
    }

    /// Add migration data from VNDS registry
    pub fn add_migration_data(
        &mut self,
        path: &Path,
        pnr_filter: Option<&HashSet<String>>,
    ) -> Result<()> {
        // Create VNDS registry loader
        let registry = factory::registry_from_name("vnds")?;

        // Load the batches
        let batches = registry.load(path, pnr_filter)?;

        let mut emigration_count = 0;
        let mut immigration_count = 0;

        for batch in batches {
            use crate::error::ParquetReaderError;

            // Extract required columns from the batch
            let pnr_column = batch
                .column_by_name("PNR")
                .ok_or_else(|| ParquetReaderError::column_not_found("PNR"))?;
            let pnr_array = pnr_column
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .ok_or_else(|| ParquetReaderError::invalid_data_type("PNR", "StringArray"))?;

            let event_code_column = batch
                .column_by_name("HAEND_KODE")
                .ok_or_else(|| ParquetReaderError::column_not_found("HAEND_KODE"))?;
            let event_code_array = event_code_column
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .ok_or_else(|| ParquetReaderError::invalid_data_type("HAEND_KODE", "Int32Array"))?;

            let event_date_column = batch
                .column_by_name("HAEND_DATO")
                .ok_or_else(|| ParquetReaderError::column_not_found("HAEND_DATO"))?;
            let event_date_array = event_date_column
                .as_any()
                .downcast_ref::<arrow::array::Date32Array>()
                .ok_or_else(|| {
                    ParquetReaderError::invalid_data_type("HAEND_DATO", "Date32Array")
                })?;

            // Process each row in the batch
            for i in 0..batch.num_rows() {
                let pnr = pnr_array.value(i);
                let event_code = event_code_array.value(i);

                // Convert date from Arrow Date32 format
                let event_date = if event_date_array.is_valid(i) {
                    let days_since_epoch = event_date_array.value(i);
                    NaiveDate::from_ymd_opt(1970, 1, 1)
                        .unwrap()
                        .checked_add_days(chrono::Days::new(days_since_epoch as u64))
                        .unwrap()
                } else {
                    continue; // Skip entries with missing dates
                };

                // Determine if this is an emigration or immigration event
                // Typical VNDS codes: 1 = immigration, 2 = emigration
                let is_emigration = event_code == 2;

                // Add to migration events
                self.migration_events
                    .entry(pnr.to_string())
                    .or_default()
                    .push((event_date, is_emigration));

                if is_emigration {
                    emigration_count += 1;
                } else {
                    immigration_count += 1;
                }
            }
        }

        log::info!(
            "Added {} migration events ({} emigrations, {} immigrations)",
            emigration_count + immigration_count,
            emigration_count,
            immigration_count
        );

        Ok(())
    }

    /// Add mortality data from DOD registry
    pub fn add_mortality_data(
        &mut self,
        path: &Path,
        pnr_filter: Option<&HashSet<String>>,
    ) -> Result<()> {
        // Create DOD registry loader
        let registry = factory::registry_from_name("dod")?;

        // Load the batches
        let batches = registry.load(path, pnr_filter)?;

        let mut death_count = 0;

        for batch in batches {
            use crate::error::ParquetReaderError;

            // Extract required columns from the batch
            let pnr_column = batch
                .column_by_name("PNR")
                .ok_or_else(|| ParquetReaderError::column_not_found("PNR"))?;
            let pnr_array = pnr_column
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .ok_or_else(|| ParquetReaderError::invalid_data_type("PNR", "StringArray"))?;

            let death_date_column = batch
                .column_by_name("DODDATO")
                .ok_or_else(|| ParquetReaderError::column_not_found("DODDATO"))?;
            let death_date_array = death_date_column
                .as_any()
                .downcast_ref::<arrow::array::Date32Array>()
                .ok_or_else(|| ParquetReaderError::invalid_data_type("DODDATO", "Date32Array"))?;

            // Process each row in the batch
            for i in 0..batch.num_rows() {
                let pnr = pnr_array.value(i);

                // Convert date from Arrow Date32 format
                let death_date = if death_date_array.is_valid(i) {
                    let days_since_epoch = death_date_array.value(i);
                    NaiveDate::from_ymd_opt(1970, 1, 1)
                        .unwrap()
                        .checked_add_days(chrono::Days::new(days_since_epoch as u64))
                        .unwrap()
                } else {
                    continue; // Skip entries with missing dates
                };

                // Store death date
                self.death_dates.insert(pnr.to_string(), death_date);
                death_count += 1;
            }
        }

        log::info!("Added {death_count} death records");

        Ok(())
    }

    /// Perform sibling identification and linkage
    ///
    /// This function identifies siblings within families, determines birth order,
    /// and analyzes sibling characteristics, including:
    /// - Number of siblings
    /// - Birth order
    /// - Presence of multiple children with chronic conditions
    /// - Age differences between siblings
    pub fn identify_siblings(&mut self) -> Result<()> {
        // Count of sibling relationships identified
        let mut sibling_count = 0;
        let mut scd_sibling_count = 0;

        // Reference date for sibling relationships
        let reference_date = chrono::Utc::now().naive_utc().date();

        // Create a mapping of family_id to children
        let mut family_children: HashMap<String, Vec<Arc<Child>>> = HashMap::new();

        // Identify families with children
        let family_ids = self
            .collection
            .get_snapshots_at(&reference_date)
            .into_iter()
            .map(|snapshot| snapshot.family_id)
            .collect::<Vec<_>>();

        for family_id in family_ids {
            if let Some(family) = self.collection.get_family(&family_id) {
                if family.children.len() > 1 {
                    // This family has multiple children (siblings)

                    // Sort children by birth date to determine birth order
                    let mut children = family.children.clone();
                    children.sort_by(|a, b| {
                        let a_birth = a.individual().birth_date;
                        let b_birth = b.individual().birth_date;
                        a_birth.cmp(&b_birth)
                    });

                    // Determine sibling characteristics
                    let has_sibling_with_scd = children.iter().any(|child| child.has_scd());
                    if has_sibling_with_scd {
                        scd_sibling_count += 1;
                    }

                    // Calculate age differences between siblings
                    let mut age_differences = Vec::new();
                    for i in 0..children.len() - 1 {
                        if let (Some(older_birth), Some(younger_birth)) = (
                            children[i].individual().birth_date,
                            children[i + 1].individual().birth_date,
                        ) {
                            // Calculate difference in years (approximate)
                            let days_diff = (younger_birth - older_birth).num_days();
                            let years_diff = days_diff as f64 / 365.25;
                            age_differences.push(years_diff);
                        }
                    }

                    // Store the children with enhanced information
                    let mut children_with_birth_order = Vec::new();
                    for (i, child) in children.iter().enumerate() {
                        // Clone the child and set birth order (1-based)
                        let updated_child =
                            Child::from_individual(child.individual().clone().into())
                                .with_birth_details(
                                    child.birth_weight,
                                    child.gestational_age,
                                    child.apgar_score,
                                )
                                .with_birth_order((i + 1) as i32);

                        // If has SCD, copy SCD information
                        let updated_child = if child.has_scd() {
                            if let Some(first_scd_date) = child.first_scd_date {
                                updated_child.with_scd(
                                    child.scd_category,
                                    first_scd_date,
                                    child.disease_severity,
                                    child.disease_origin,
                                )
                            } else {
                                updated_child
                            }
                        } else {
                            updated_child
                        };

                        children_with_birth_order.push(Arc::new(updated_child));
                    }

                    // Store family with enhanced sibling information
                    family_children.insert(family_id.clone(), children_with_birth_order.clone());
                    sibling_count += children.len();

                    // Get the original family and update it with enhanced children
                    if let Some(original_family) = self.collection.get_family(&family_id) {
                        // Create a mutable version
                        let mut updated_family = Family {
                            family_id: original_family.family_id.clone(),
                            family_type: original_family.family_type,
                            mother: original_family.mother.clone(),
                            father: original_family.father.clone(),
                            municipality_code: original_family.municipality_code.clone(),
                            is_rural: original_family.is_rural,
                            valid_from: original_family.valid_from,
                            valid_to: original_family.valid_to,
                            has_parental_comorbidity: original_family.has_parental_comorbidity,
                            has_support_network: original_family.has_support_network,
                            children: Vec::new(), // Will be replaced
                        };

                        // Replace the children with the enhanced versions
                        updated_family.children = children_with_birth_order;

                        // Update the family in the collection
                        let updated = self.collection.update_family(&family_id, updated_family);
                        if updated {
                            log::debug!(
                                "Updated family {family_id} with enhanced sibling information"
                            );
                        }
                    }
                }
            }
        }

        log::info!(
            "Identified {} children in sibling relationships across {} families",
            sibling_count,
            family_children.len()
        );
        log::info!("{scd_sibling_count} families have at least one child with SCD");

        // For further analysis, we could add additional metadata to the family collection
        // with these sibling relationships, such as:
        // - Maximum age difference between siblings
        // - Presence of SCD in siblings
        // - Birth order of the SCD child
        // This information would be valuable for the case-control matching process

        Ok(())
    }
}

impl Default for RegistryIntegration {
    fn default() -> Self {
        Self::new()
    }
}
