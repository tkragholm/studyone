#[cfg(test)]
mod tests {
    use par_reader::models::child::{Child, ChildCollection, DiseaseSeverity, DiseaseOrigin, ScdCategory};
    use par_reader::models::diagnosis::{Diagnosis, DiagnosisType};
    use par_reader::models::individual::{EducationLevel, Gender, Individual, Origin};
    use std::sync::Arc;
    use chrono::NaiveDate;

    /// Create a test individual for a child
    fn create_test_individual() -> Individual {
        Individual {
            pnr: "1234567890".to_string(),
            gender: Gender::Male,
            birth_date: Some(NaiveDate::from_ymd_opt(2010, 6, 15).unwrap()),
            death_date: None,
            origin: Origin::Danish,
            education_level: EducationLevel::Unknown,
            municipality_code: Some("101".to_string()),
            is_rural: false,
            mother_pnr: Some("1111111111".to_string()),
            father_pnr: Some("2222222222".to_string()),
            family_id: Some("FAM123".to_string()),
            emigration_date: None,
            immigration_date: None,
        }
    }

    #[test]
    fn test_child_creation() {
        let individual = Arc::new(create_test_individual());
        let child = Child::from_individual(individual.clone());

        assert_eq!(child.individual().pnr, "1234567890");
        assert!(!child.has_severe_chronic_disease);
        assert_eq!(child.scd_category, ScdCategory::None);
        assert_eq!(child.disease_severity, DiseaseSeverity::None);
        assert_eq!(child.disease_origin, DiseaseOrigin::None);
        assert!(!child.is_index_case);
    }

    #[test]
    fn test_child_with_birth_details() {
        let individual = Arc::new(create_test_individual());
        let child = Child::from_individual(individual.clone())
            .with_birth_details(Some(3500), Some(40), Some(10))
            .with_birth_order(1);

        assert_eq!(child.birth_weight, Some(3500));
        assert_eq!(child.gestational_age, Some(40));
        assert_eq!(child.apgar_score, Some(10));
        assert_eq!(child.birth_order, Some(1));
    }

    #[test]
    fn test_child_with_scd() {
        let individual = Arc::new(create_test_individual());
        let diagnosis_date = NaiveDate::from_ymd_opt(2015, 3, 10).unwrap();

        let child = Child::from_individual(individual.clone())
            .with_scd(
                ScdCategory::RespiratoryDisorder,
                diagnosis_date,
                DiseaseSeverity::Moderate,
                DiseaseOrigin::Acquired,
            )
            .as_index_case();

        assert!(child.has_severe_chronic_disease);
        assert_eq!(child.scd_category, ScdCategory::RespiratoryDisorder);
        assert_eq!(child.first_scd_date, Some(diagnosis_date));
        assert_eq!(child.disease_severity, DiseaseSeverity::Moderate);
        assert_eq!(child.disease_origin, DiseaseOrigin::Acquired);
        assert!(child.is_index_case);
        assert!(child.had_scd_at(&NaiveDate::from_ymd_opt(2020, 1, 1).unwrap()));
        assert!(!child.had_scd_at(&NaiveDate::from_ymd_opt(2014, 1, 1).unwrap()));
    }

    #[test]
    fn test_add_diagnosis() {
        let individual = Arc::new(create_test_individual());
        let mut child = Child::from_individual(individual.clone());

        // Initially no SCD
        assert!(!child.has_severe_chronic_disease);

        // Add a non-SCD diagnosis
        let diagnosis1 = Diagnosis {
            individual_pnr: "1234567890".to_string(),
            diagnosis_code: "J20".to_string(),
            diagnosis_type: DiagnosisType::Secondary,
            diagnosis_date: Some(NaiveDate::from_ymd_opt(2015, 3, 10).unwrap()),
            is_scd: false,
            severity: 1,
        };

        child.add_diagnosis(Arc::new(diagnosis1));

        // Should still not have SCD
        assert!(!child.has_severe_chronic_disease);
        assert_eq!(child.diagnoses.len(), 1);

        // Add an SCD diagnosis
        let diagnosis2 = Diagnosis {
            individual_pnr: "1234567890".to_string(),
            diagnosis_code: "J45".to_string(),
            diagnosis_type: DiagnosisType::Primary,
            diagnosis_date: Some(NaiveDate::from_ymd_opt(2016, 5, 20).unwrap()),
            is_scd: true,
            severity: 2,
        };

        child.add_diagnosis(Arc::new(diagnosis2));

        // Should now have SCD
        assert!(child.has_severe_chronic_disease);
        assert_eq!(child.diagnoses.len(), 2);
        assert_eq!(
            child.first_scd_date,
            Some(NaiveDate::from_ymd_opt(2016, 5, 20).unwrap())
        );

        // Add an earlier SCD diagnosis
        let diagnosis3 = Diagnosis {
            individual_pnr: "1234567890".to_string(),
            diagnosis_code: "E10".to_string(),
            diagnosis_type: DiagnosisType::Primary,
            diagnosis_date: Some(NaiveDate::from_ymd_opt(2015, 7, 15).unwrap()),
            is_scd: true,
            severity: 3,
        };

        child.add_diagnosis(Arc::new(diagnosis3));

        // First SCD date should be updated
        assert_eq!(child.diagnoses.len(), 3);
        assert_eq!(
            child.first_scd_date,
            Some(NaiveDate::from_ymd_opt(2015, 7, 15).unwrap())
        );
    }

    #[test]
    fn test_age_at_onset() {
        let individual = Arc::new(create_test_individual());
        let diagnosis_date = NaiveDate::from_ymd_opt(2015, 6, 15).unwrap();

        let child = Child::from_individual(individual.clone()).with_scd(
            ScdCategory::RespiratoryDisorder,
            diagnosis_date,
            DiseaseSeverity::Moderate,
            DiseaseOrigin::Acquired,
        );

        // Child born on 2010-06-15, diagnosed on 2015-06-15, so exactly 5 years old at onset
        assert_eq!(child.age_at_onset(), Some(5));
    }

    #[test]
    fn test_child_collection() {
        let mut collection = ChildCollection::new();

        // Create children
        let individual1 = Arc::new(Individual {
            pnr: "1111111111".to_string(),
            gender: Gender::Male,
            birth_date: Some(NaiveDate::from_ymd_opt(2010, 1, 1).unwrap()),
            death_date: None,
            origin: Origin::Danish,
            education_level: EducationLevel::Unknown,
            municipality_code: Some("101".to_string()),
            is_rural: false,
            mother_pnr: Some("MOTHER1".to_string()),
            father_pnr: Some("FATHER1".to_string()),
            family_id: Some("FAM1".to_string()),
            emigration_date: None,
            immigration_date: None,
        });

        let individual2 = Arc::new(Individual {
            pnr: "2222222222".to_string(),
            gender: Gender::Female,
            birth_date: Some(NaiveDate::from_ymd_opt(2012, 1, 1).unwrap()),
            death_date: None,
            origin: Origin::Danish,
            education_level: EducationLevel::Unknown,
            municipality_code: Some("101".to_string()),
            is_rural: false,
            mother_pnr: Some("MOTHER2".to_string()),
            father_pnr: Some("FATHER2".to_string()),
            family_id: Some("FAM2".to_string()),
            emigration_date: None,
            immigration_date: None,
        });

        let child1 = Child::from_individual(individual1.clone()).with_scd(
            ScdCategory::RespiratoryDisorder,
            NaiveDate::from_ymd_opt(2015, 3, 10).unwrap(),
            DiseaseSeverity::Moderate,
            DiseaseOrigin::Acquired,
        );

        let child2 = Child::from_individual(individual2.clone());

        // Add to collection
        collection.add_child(child1);
        collection.add_child(child2);

        // Test collection
        assert_eq!(collection.count(), 2);
        assert!(collection.get_child("1111111111").is_some());
        assert!(collection.get_child("2222222222").is_some());
        assert!(collection.get_child("3333333333").is_none());

        // Test filtering
        let with_scd = collection.children_with_scd();
        assert_eq!(with_scd.len(), 1);
        assert_eq!(with_scd[0].individual().pnr, "1111111111");

        let without_scd = collection.children_without_scd();
        assert_eq!(without_scd.len(), 1);
        assert_eq!(without_scd[0].individual().pnr, "2222222222");

        let with_scd_at_date =
            collection.children_with_scd_at(&NaiveDate::from_ymd_opt(2016, 1, 1).unwrap());
        assert_eq!(with_scd_at_date.len(), 1);

        let with_respiratory =
            collection.children_with_scd_category(ScdCategory::RespiratoryDisorder);
        assert_eq!(with_respiratory.len(), 1);
    }
}
