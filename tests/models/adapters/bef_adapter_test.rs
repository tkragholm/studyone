#[cfg(test)]
mod tests {
    use arrow::array::{ArrayRef, Date32Builder, Int8Builder, StringBuilder};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use chrono::NaiveDate;
    use par_reader::RegistryAdapter;
    use par_reader::models::adapters::bef_adapter::*;
    use par_reader::models::family::FamilyType;
    use par_reader::models::individual::{Gender, Origin};
    use std::sync::Arc;

    fn create_test_batch() -> RecordBatch {
        // Create schema
        let schema = Schema::new(vec![
            Field::new("PNR", DataType::Utf8, false),
            Field::new("FOED_DAG", DataType::Date32, true),
            Field::new("FAR_ID", DataType::Utf8, true),
            Field::new("MOR_ID", DataType::Utf8, true),
            Field::new("FAMILIE_ID", DataType::Utf8, true),
            Field::new("KOEN", DataType::Utf8, true),
            Field::new("KOM", DataType::Int8, true),
            Field::new("OPR_LAND", DataType::Utf8, true),
        ]);

        // Build string arrays
        let mut pnr_builder = StringBuilder::new();
        pnr_builder.append_value("1234567890"); // Father
        pnr_builder.append_value("2345678901"); // Mother
        pnr_builder.append_value("3456789012"); // Child
        let pnr_array = Arc::new(pnr_builder.finish()) as ArrayRef;

        // Build birth date array
        let mut birth_day_builder = Date32Builder::new();
        // Convert dates to days since epoch (1970-01-01)
        let date1 = NaiveDate::from_ymd_opt(1980, 1, 15).unwrap();
        let date2 = NaiveDate::from_ymd_opt(1982, 5, 22).unwrap();
        let date3 = NaiveDate::from_ymd_opt(2010, 8, 10).unwrap();
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        birth_day_builder.append_value((date1 - epoch).num_days() as i32);
        birth_day_builder.append_value((date2 - epoch).num_days() as i32);
        birth_day_builder.append_value((date3 - epoch).num_days() as i32);
        let birth_day_array = Arc::new(birth_day_builder.finish()) as ArrayRef;

        // Build other string arrays
        let mut far_id_builder = StringBuilder::new();
        far_id_builder.append_null();
        far_id_builder.append_null();
        far_id_builder.append_value("1234567890");
        let far_id_array = Arc::new(far_id_builder.finish()) as ArrayRef;

        let mut mor_id_builder = StringBuilder::new();
        mor_id_builder.append_null();
        mor_id_builder.append_null();
        mor_id_builder.append_value("2345678901");
        let mor_id_array = Arc::new(mor_id_builder.finish()) as ArrayRef;

        let mut familie_id_builder = StringBuilder::new();
        familie_id_builder.append_value("FAM12345");
        familie_id_builder.append_value("FAM12345");
        familie_id_builder.append_value("FAM12345");
        let familie_id_array = Arc::new(familie_id_builder.finish()) as ArrayRef;

        let mut gender_builder = StringBuilder::new();
        gender_builder.append_value("M");
        gender_builder.append_value("F");
        gender_builder.append_value("M");
        let gender_array = Arc::new(gender_builder.finish()) as ArrayRef;

        // Build municipality array
        let mut kom_builder = Int8Builder::new();
        kom_builder.append_value(101);
        kom_builder.append_value(101);
        kom_builder.append_value(101);
        let kom_array = Arc::new(kom_builder.finish()) as ArrayRef;

        // Build origin array
        let mut origin_builder = StringBuilder::new();
        origin_builder.append_value("5100"); // Danish
        origin_builder.append_value("5100"); // Danish
        origin_builder.append_value("5100"); // Danish
        let origin_array = Arc::new(origin_builder.finish()) as ArrayRef;

        // Create record batch
        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                pnr_array,
                birth_day_array,
                far_id_array,
                mor_id_array,
                familie_id_array,
                gender_array,
                kom_array,
                origin_array,
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_bef_individual_adapter() {
        let batch = create_test_batch();
        let result = BefIndividualAdapter::from_record_batch(&batch);
        assert!(result.is_ok());

        let individuals = result.unwrap();
        assert_eq!(individuals.len(), 3);

        // Check first individual (father)
        let father = &individuals[0];
        assert_eq!(father.pnr, "1234567890");
        assert_eq!(father.gender, Gender::Male);
        assert_eq!(father.origin, Origin::Danish);
        assert!(father.family_id.is_some());
        assert_eq!(father.family_id.as_ref().unwrap(), "FAM12345");

        // Check second individual (mother)
        let mother = &individuals[1];
        assert_eq!(mother.pnr, "2345678901");
        assert_eq!(mother.gender, Gender::Female);

        // Check third individual (child)
        let child = &individuals[2];
        assert_eq!(child.pnr, "3456789012");
        assert_eq!(child.father_pnr.as_ref().unwrap(), "1234567890");
        assert_eq!(child.mother_pnr.as_ref().unwrap(), "2345678901");
    }

    #[test]
    fn test_bef_family_adapter() {
        let batch = create_test_batch();
        let result = BefFamilyAdapter::from_record_batch(&batch);
        assert!(result.is_ok());

        let families = result.unwrap();
        assert_eq!(families.len(), 1);

        let family = &families[0];
        assert_eq!(family.family_id, "FAM12345");
        assert_eq!(family.family_type, FamilyType::TwoParent);
    }
}
