#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use par_reader::models::diagnosis::*;
    use std::sync::Arc;

    #[test]
    fn test_diagnosis_creation() {
        let diagnosis = Diagnosis::new(
            "1234567890".to_string(),
            "J45".to_string(),
            DiagnosisType::Primary,
            Some(NaiveDate::from_ymd_opt(2015, 3, 10).unwrap()),
        );

        assert_eq!(diagnosis.individual_pnr, "1234567890");
        assert_eq!(diagnosis.diagnosis_code, "J45");
        assert_eq!(diagnosis.diagnosis_type, DiagnosisType::Primary);
        assert_eq!(
            diagnosis.diagnosis_date,
            Some(NaiveDate::from_ymd_opt(2015, 3, 10).unwrap())
        );
        assert!(!diagnosis.is_scd);
        assert_eq!(diagnosis.severity, 1);
    }

    #[test]
    fn test_diagnosis_as_scd() {
        let diagnosis = Diagnosis::new(
            "1234567890".to_string(),
            "J45".to_string(),
            DiagnosisType::Primary,
            Some(NaiveDate::from_ymd_opt(2015, 3, 10).unwrap()),
        )
        .as_scd(2);

        assert!(diagnosis.is_scd);
        assert_eq!(diagnosis.severity, 2);
    }

    #[test]
    fn test_is_in_chapter() {
        // Respiratory system (Chapter X)
        let respiratory = Diagnosis::new(
            "1234567890".to_string(),
            "J45".to_string(),
            DiagnosisType::Primary,
            None,
        );
        assert!(respiratory.is_in_chapter("X"));
        assert!(!respiratory.is_in_chapter("IX"));

        // Circulatory system (Chapter IX)
        let circulatory = Diagnosis::new(
            "1234567890".to_string(),
            "I50".to_string(),
            DiagnosisType::Primary,
            None,
        );
        assert!(circulatory.is_in_chapter("IX"));
        assert!(!circulatory.is_in_chapter("X"));

        // Congenital malformations (Chapter XVII)
        let congenital = Diagnosis::new(
            "1234567890".to_string(),
            "Q21.0".to_string(),
            DiagnosisType::Primary,
            None,
        );
        assert!(congenital.is_in_chapter("XVII"));
        assert!(!congenital.is_in_chapter("XVI"));
    }

    #[test]
    fn test_matches_code() {
        let diagnosis = Diagnosis::new(
            "1234567890".to_string(),
            "J45.0".to_string(),
            DiagnosisType::Primary,
            None,
        );

        // Exact match
        assert!(diagnosis.matches_code("J45.0"));

        // Prefix match
        assert!(diagnosis.matches_code("J45*"));
        assert!(diagnosis.matches_code("J*"));

        // Non-matches
        assert!(!diagnosis.matches_code("J46"));
        assert!(!diagnosis.matches_code("J45.1"));
    }

    #[test]
    fn test_scd_result() {
        let mut result = ScdResult::new("1234567890".to_string());

        // Initially no SCD
        assert!(!result.has_scd);
        assert!(result.first_scd_date.is_none());
        assert!(result.scd_diagnoses.is_empty());
        assert!(result.scd_categories.is_empty());
        assert_eq!(result.max_severity, 0);
        assert!(!result.has_congenital);

        // Add an SCD diagnosis
        let diagnosis1 = Arc::new(
            Diagnosis::new(
                "1234567890".to_string(),
                "J45".to_string(),
                DiagnosisType::Primary,
                Some(NaiveDate::from_ymd_opt(2015, 3, 10).unwrap()),
            )
            .as_scd(2),
        );

        result.add_scd_diagnosis(diagnosis1, 6, false); // Category 6 = Respiratory

        // Check update
        assert!(result.has_scd);
        assert_eq!(
            result.first_scd_date,
            Some(NaiveDate::from_ymd_opt(2015, 3, 10).unwrap())
        );
        assert_eq!(result.scd_diagnoses.len(), 1);
        assert_eq!(result.scd_categories, vec![6]);
        assert_eq!(result.max_severity, 2);
        assert!(!result.has_congenital);

        // Add another SCD diagnosis with different category
        let diagnosis2 = Arc::new(
            Diagnosis::new(
                "1234567890".to_string(),
                "Q21.0".to_string(),
                DiagnosisType::Primary,
                Some(NaiveDate::from_ymd_opt(2010, 1, 15).unwrap()),
            )
            .as_scd(3),
        );

        result.add_scd_diagnosis(diagnosis2, 10, true); // Category 10 = Congenital

        // Check update with congenital and earlier date
        assert!(result.has_scd);
        assert_eq!(
            result.first_scd_date,
            Some(NaiveDate::from_ymd_opt(2010, 1, 15).unwrap())
        );
        assert_eq!(result.scd_diagnoses.len(), 2);
        assert_eq!(result.scd_categories, vec![6, 10]);
        assert_eq!(result.max_severity, 3);
        assert!(result.has_congenital);

        // Test category methods
        assert!(result.has_category(6));
        assert!(result.has_category(10));
        assert!(!result.has_category(5));
        assert_eq!(result.category_count(), 2);

        // Test severity
        result.add_hospitalizations(5);
        assert_eq!(result.hospitalization_count, 5);
        assert_eq!(result.hospitalization_severity(), 3);
        assert_eq!(result.combined_severity(), 3);
    }

    #[test]
    fn test_diagnosis_collection() {
        let mut collection = DiagnosisCollection::new();

        // Add diagnoses
        let diagnosis1 = Diagnosis::new(
            "1111111111".to_string(),
            "J45".to_string(),
            DiagnosisType::Primary,
            Some(NaiveDate::from_ymd_opt(2015, 3, 10).unwrap()),
        )
        .as_scd(2);

        let diagnosis2 = Diagnosis::new(
            "1111111111".to_string(),
            "I50".to_string(),
            DiagnosisType::Secondary,
            Some(NaiveDate::from_ymd_opt(2016, 5, 20).unwrap()),
        )
        .as_scd(3);

        let diagnosis3 = Diagnosis::new(
            "2222222222".to_string(),
            "K90".to_string(),
            DiagnosisType::Primary,
            Some(NaiveDate::from_ymd_opt(2014, 7, 15).unwrap()),
        )
        .as_scd(2);

        collection.add_diagnosis(diagnosis1);
        collection.add_diagnosis(diagnosis2);
        collection.add_diagnosis(diagnosis3);

        // Check retrieval
        let diagnoses1 = collection.get_diagnoses("1111111111");
        assert_eq!(diagnoses1.len(), 2);

        let diagnoses2 = collection.get_diagnoses("2222222222");
        assert_eq!(diagnoses2.len(), 1);

        // Add SCD results
        let mut result1 = ScdResult::new("1111111111".to_string());
        result1.has_scd = true;
        result1.scd_categories = vec![6, 9]; // Respiratory, Cardiovascular
        result1.max_severity = 3;

        let mut result2 = ScdResult::new("2222222222".to_string());
        result2.has_scd = true;
        result2.scd_categories = vec![7]; // Gastrointestinal
        result2.max_severity = 2;

        let result3 = ScdResult::new("3333333333".to_string());

        collection.add_scd_result(result1);
        collection.add_scd_result(result2);
        collection.add_scd_result(result3);

        // Test SCD queries
        let with_scd = collection.individuals_with_scd();
        assert_eq!(with_scd.len(), 2);
        assert!(with_scd.contains(&"1111111111".to_string()));
        assert!(with_scd.contains(&"2222222222".to_string()));

        let without_scd = collection.individuals_without_scd();
        assert_eq!(without_scd.len(), 1);
        assert!(without_scd.contains(&"3333333333".to_string()));

        let with_respiratory = collection.individuals_with_category(6);
        assert_eq!(with_respiratory.len(), 1);
        assert!(with_respiratory.contains(&"1111111111".to_string()));

        let with_gi = collection.individuals_with_category(7);
        assert_eq!(with_gi.len(), 1);
        assert!(with_gi.contains(&"2222222222".to_string()));

        // Test counts
        assert_eq!(collection.scd_count(), 2);

        let by_severity = collection.count_by_severity();
        assert_eq!(by_severity.get(&3), Some(&1));
        assert_eq!(by_severity.get(&2), Some(&1));
    }
}
