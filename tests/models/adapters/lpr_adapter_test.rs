#[cfg(test)]
mod tests {
    use arrow::array::{ArrayRef, Date32Builder, StringBuilder};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use chrono::NaiveDate;
    use par_reader::models::adapters::lpr_adapter::*;
    use par_reader::models::diagnosis::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn create_test_lpr2_batch() -> RecordBatch {
        // Create schema
        let schema = Schema::new(vec![
            Field::new("RECNUM", DataType::Utf8, false),
            Field::new("C_DIAG", DataType::Utf8, true),
            Field::new("C_DIAGTYPE", DataType::Utf8, true),
            Field::new("LEVERANCEDATO", DataType::Date32, true),
        ]);

        // Build string arrays
        let mut recnum_builder = StringBuilder::new();
        recnum_builder.append_value("REC123");
        recnum_builder.append_value("REC456");
        let recnum_array = Arc::new(recnum_builder.finish()) as ArrayRef;

        let mut diag_builder = StringBuilder::new();
        diag_builder.append_value("E10"); // Type 1 diabetes (an SCD)
        diag_builder.append_value("J20"); // Bronchitis (not an SCD)
        let diag_array = Arc::new(diag_builder.finish()) as ArrayRef;

        let mut diagtype_builder = StringBuilder::new();
        diagtype_builder.append_value("A"); // Primary diagnosis
        diagtype_builder.append_value("B"); // Secondary diagnosis
        let diagtype_array = Arc::new(diagtype_builder.finish()) as ArrayRef;

        // Build date array
        let mut date_builder = Date32Builder::new();
        let date1 = NaiveDate::from_ymd_opt(2020, 5, 15).unwrap();
        let date2 = NaiveDate::from_ymd_opt(2020, 6, 20).unwrap();
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        date_builder.append_value((date1 - epoch).num_days() as i32);
        date_builder.append_value((date2 - epoch).num_days() as i32);
        let date_array = Arc::new(date_builder.finish()) as ArrayRef;

        // Create record batch
        RecordBatch::try_new(
            Arc::new(schema),
            vec![recnum_array, diag_array, diagtype_array, date_array],
        )
        .unwrap()
    }

    fn create_test_lpr3_batch() -> RecordBatch {
        // Create schema
        let schema = Schema::new(vec![
            Field::new("DW_EK_KONTAKT", DataType::Utf8, false),
            Field::new("diagnosekode", DataType::Utf8, true),
            Field::new("diagnosetype", DataType::Utf8, true),
            Field::new("senere_afkraeftet", DataType::Utf8, true),
        ]);

        // Build string arrays
        let mut kontakt_builder = StringBuilder::new();
        kontakt_builder.append_value("KONTAKT123");
        kontakt_builder.append_value("KONTAKT456");
        let kontakt_array = Arc::new(kontakt_builder.finish()) as ArrayRef;

        let mut diag_builder = StringBuilder::new();
        diag_builder.append_value("C50"); // Breast cancer (an SCD)
        diag_builder.append_value("M79"); // Soft tissue disorder (not an SCD)
        let diag_array = Arc::new(diag_builder.finish()) as ArrayRef;

        let mut diagtype_builder = StringBuilder::new();
        diagtype_builder.append_value("A"); // Primary diagnosis
        diagtype_builder.append_value("B"); // Secondary diagnosis
        let diagtype_array = Arc::new(diagtype_builder.finish()) as ArrayRef;

        let mut afkraeftet_builder = StringBuilder::new();
        afkraeftet_builder.append_value("NEJ"); // Not disproven
        afkraeftet_builder.append_value("NEJ"); // Not disproven
        let afkraeftet_array = Arc::new(afkraeftet_builder.finish()) as ArrayRef;

        // Create record batch
        RecordBatch::try_new(
            Arc::new(schema),
            vec![kontakt_array, diag_array, diagtype_array, afkraeftet_array],
        )
        .unwrap()
    }

    #[test]
    fn test_lpr2_adapter() {
        let batch = create_test_lpr2_batch();

        // Create PNR lookup
        let mut pnr_lookup = HashMap::new();
        pnr_lookup.insert("REC123".to_string(), "1234567890".to_string());
        pnr_lookup.insert("REC456".to_string(), "2345678901".to_string());

        let adapter = Lpr2DiagAdapter::new(pnr_lookup);
        let result = adapter.process_batch(&batch);

        assert!(result.is_ok());
        let diagnoses = result.unwrap();

        // Check that we got both diagnoses
        assert_eq!(diagnoses.len(), 2);

        // Check that the first diagnosis is correctly identified as an SCD
        let scd_diag = diagnoses
            .iter()
            .find(|d| d.diagnosis_code == "E10")
            .unwrap();
        assert!(scd_diag.is_scd);
        assert_eq!(scd_diag.severity, 2); // Moderate severity

        // Check that the second diagnosis is not an SCD
        let non_scd_diag = diagnoses
            .iter()
            .find(|d| d.diagnosis_code == "J20")
            .unwrap();
        assert!(!non_scd_diag.is_scd);
        assert_eq!(non_scd_diag.severity, 1); // Minimal severity
    }

    #[test]
    fn test_lpr3_adapter() {
        let batch = create_test_lpr3_batch();

        // Create PNR lookup
        let mut pnr_lookup = HashMap::new();
        pnr_lookup.insert("KONTAKT123".to_string(), "1234567890".to_string());
        pnr_lookup.insert("KONTAKT456".to_string(), "2345678901".to_string());

        let adapter = Lpr3DiagnoserAdapter::new(pnr_lookup);
        let result = adapter.process_batch(&batch);

        assert!(result.is_ok());
        let diagnoses = result.unwrap();

        // Check that we got both diagnoses
        assert_eq!(diagnoses.len(), 2);

        // Check that the first diagnosis is correctly identified as an SCD
        let scd_diag = diagnoses
            .iter()
            .find(|d| d.diagnosis_code == "C50")
            .unwrap();
        assert!(scd_diag.is_scd);
        assert_eq!(scd_diag.severity, 3); // High severity for cancer

        // Check that the second diagnosis is not an SCD
        let non_scd_diag = diagnoses
            .iter()
            .find(|d| d.diagnosis_code == "M79")
            .unwrap();
        assert!(!non_scd_diag.is_scd);
    }

    #[test]
    fn test_scd_processing() {
        // Create some test diagnoses
        let diagnoses = vec![
            Diagnosis::new(
                "1234567890".to_string(),
                "E10".to_string(),
                DiagnosisType::Primary,
                Some(NaiveDate::from_ymd_opt(2020, 5, 15).unwrap()),
            )
            .as_scd(2),
            Diagnosis::new(
                "1234567890".to_string(),
                "J20".to_string(),
                DiagnosisType::Secondary,
                Some(NaiveDate::from_ymd_opt(2020, 6, 20).unwrap()),
            ),
            Diagnosis::new(
                "2345678901".to_string(),
                "C50".to_string(),
                DiagnosisType::Primary,
                Some(NaiveDate::from_ymd_opt(2019, 8, 10).unwrap()),
            )
            .as_scd(3),
        ];

        let adapter = LprBaseAdapter::new();
        let results = adapter.process_scd_results(&diagnoses);

        // Check that we have SCD results for both individuals
        assert_eq!(results.len(), 2);

        // Check the first individual's SCD result
        let result1 = results.get("1234567890").unwrap();
        assert!(result1.has_scd);
        assert_eq!(result1.scd_diagnoses.len(), 1);
        assert_eq!(result1.max_severity, 2);

        // Check the second individual's SCD result
        let result2 = results.get("2345678901").unwrap();
        assert!(result2.has_scd);
        assert_eq!(result2.scd_diagnoses.len(), 1);
        assert_eq!(result2.max_severity, 3);
    }
}
