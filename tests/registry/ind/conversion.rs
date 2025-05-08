#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringBuilder;
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatchBuilder;

    #[test]
    fn test_individual_from_ind_record() -> Result<()> {
        // Create a test batch with IND data
        let schema = Schema::new(vec![
            Field::new("PNR", DataType::Utf8, false),
            // Add other fields as needed
        ]);

        let mut batch_builder = RecordBatchBuilder::new_with_capacity(schema, 1);

        // Add PNR data
        let mut pnr_builder = StringBuilder::new_with_capacity(1, 12);
        pnr_builder.append_value("0987654321")?;
        batch_builder
            .column_builder::<StringBuilder>(0)
            .unwrap()
            .append_builder(&pnr_builder)?;

        let batch = batch_builder.build()?;

        // Test conversion
        let individual = Individual::from_ind_record(&batch, 0)?;

        assert!(individual.is_some());
        let individual = individual.unwrap();
        assert_eq!(individual.pnr, "0987654321");

        Ok(())
    }
}
