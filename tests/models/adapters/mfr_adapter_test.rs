#[cfg(test)]
mod tests {
    use arrow::array::{ArrayRef, Date32Builder, StringBuilder};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use chrono::NaiveDate;
    use par_reader::models::adapters::*;
    use par_reader::models::individual::Gender;
    use par_reader::models::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn create_test_batch() -> RecordBatch {
        // Create schema
        let schema = Schema::new(vec![
            Field::new("CPR_BARN", DataType::Utf8, false),
            Field::new("FOEDSELSDATO", DataType::Date32, true),
            Field::new("CPR_MODER", DataType::Utf8, true),
            Field::new("CPR_FADER", DataType::Utf8, true),
        ]);

        // Build string arrays
        let mut child_pnr_builder = StringBuilder::new();
        child_pnr_builder.append_value("3456789012"); // Child 1
        child_pnr_builder.append_value("4567890123"); // Child 2
        let child_pnr_array = Arc::new(child_pnr_builder.finish()) as ArrayRef;

        // Build birth date array
        let mut birth_day_builder = Date32Builder::new();
        // Convert dates to days since epoch (1970-01-01)
        let date1 = NaiveDate::from_ymd_opt(2010, 8, 10).unwrap();
        let date2 = NaiveDate::from_ymd_opt(2012, 5, 15).unwrap();
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        birth_day_builder.append_value((date1 - epoch).num_days() as i32);
        birth_day_builder.append_value((date2 - epoch).num_days() as i32);
        let birth_day_array = Arc::new(birth_day_builder.finish()) as ArrayRef;

        // Build other string arrays
        let mut mother_pnr_builder = StringBuilder::new();
        mother_pnr_builder.append_value("2345678901");
        mother_pnr_builder.append_value("2345678901"); // Same mother for both children
        let mother_pnr_array = Arc::new(mother_pnr_builder.finish()) as ArrayRef;

        let mut father_pnr_builder = StringBuilder::new();
        father_pnr_builder.append_value("1234567890");
        father_pnr_builder.append_value("1234567890"); // Same father for both children
        let father_pnr_array = Arc::new(father_pnr_builder.finish()) as ArrayRef;

        // Create record batch
        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                child_pnr_array,
                birth_day_array,
                mother_pnr_array,
                father_pnr_array,
            ],
        )
        .unwrap()
    }

    fn create_test_individuals() -> HashMap<String, Arc<Individual>> {
        let mut individuals = HashMap::new();

        // Create father
        let father = Individual::new(
            "1234567890".to_string(),
            Gender::Male,
            Some(NaiveDate::from_ymd_opt(1980, 1, 15).unwrap()),
        );
        individuals.insert(father.pnr.clone(), Arc::new(father));

        // Create mother
        let mother = Individual::new(
            "2345678901".to_string(),
            Gender::Female,
            Some(NaiveDate::from_ymd_opt(1982, 5, 22).unwrap()),
        );
        individuals.insert(mother.pnr.clone(), Arc::new(mother));

        // Create child 1
        let mut child1 = Individual::new(
            "3456789012".to_string(),
            Gender::Male,
            Some(NaiveDate::from_ymd_opt(2010, 8, 10).unwrap()),
        );
        child1.mother_pnr = Some("2345678901".to_string());
        child1.father_pnr = Some("1234567890".to_string());
        individuals.insert(child1.pnr.clone(), Arc::new(child1));

        // Create child 2
        let mut child2 = Individual::new(
            "4567890123".to_string(),
            Gender::Female,
            Some(NaiveDate::from_ymd_opt(2012, 5, 15).unwrap()),
        );
        child2.mother_pnr = Some("2345678901".to_string());
        child2.father_pnr = Some("1234567890".to_string());
        individuals.insert(child2.pnr.clone(), Arc::new(child2));

        individuals
    }

    #[test]
    fn test_mfr_child_adapter() {
        let batch = create_test_batch();
        let individuals = create_test_individuals();

        let adapter = MfrChildAdapter::new(individuals);
        let result = adapter.process_batch(&batch);

        assert!(result.is_ok());
        let children = result.unwrap();

        // Check that we got both children
        assert_eq!(children.len(), 2);

        // Check birth order
        let child1 = children
            .iter()
            .find(|c| c.individual().pnr == "3456789012")
            .unwrap();
        let child2 = children
            .iter()
            .find(|c| c.individual().pnr == "4567890123")
            .unwrap();

        assert_eq!(child1.birth_order, Some(1)); // First born
        assert_eq!(child2.birth_order, Some(2)); // Second born
    }
}
