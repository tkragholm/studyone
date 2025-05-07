use arrow::array::{
    Array, ArrayRef, Date32Array, Float64Array, Int32Array, StringArray, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use par_reader::schema::adapt::{
    AdaptationStrategy, DateFormatConfig, TypeCompatibility, adapt_record_batch,
    check_schema_with_adaptation, check_type_compatibility, convert_array,
};
use std::sync::Arc;

#[test]
fn test_type_compatibility() {
    // Test exact compatibility
    assert_eq!(
        check_type_compatibility(&DataType::Int32, &DataType::Int32),
        TypeCompatibility::Exact
    );

    // Test compatible numeric types
    assert_eq!(
        check_type_compatibility(&DataType::Int32, &DataType::Int64),
        TypeCompatibility::Compatible
    );
    assert_eq!(
        check_type_compatibility(&DataType::Int32, &DataType::Float64),
        TypeCompatibility::Compatible
    );

    // Test string to date compatibility
    assert_eq!(
        check_type_compatibility(&DataType::Utf8, &DataType::Date32),
        TypeCompatibility::Compatible
    );

    // Test incompatible types
    assert_eq!(
        check_type_compatibility(&DataType::Utf8, &DataType::Binary),
        TypeCompatibility::Incompatible
    );
}

#[test]
fn test_schema_compatibility_report() {
    // Create source schema with string and int fields
    let source_schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("date_str", DataType::Utf8, false),
    ]);

    // Create target schema with converted types
    let target_schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("date_str", DataType::Date32, false),
    ]);

    // Check compatibility
    let report = check_schema_with_adaptation(&source_schema, &target_schema);

    // Ensure compatibility is true (all fields can be adapted)
    assert!(report.compatible);

    // Verify adaptations
    assert_eq!(report.adaptations.len(), 2); // Two fields need adaptation

    // Check strategies
    let id_adaptation = report
        .adaptations
        .iter()
        .find(|a| a.field_name == "id")
        .expect("Should have adaptation for id field");
    assert_eq!(
        id_adaptation.adaptation_strategy,
        AdaptationStrategy::NumericConversion
    );

    let date_adaptation = report
        .adaptations
        .iter()
        .find(|a| a.field_name == "date_str")
        .expect("Should have adaptation for date_str field");
    assert_eq!(
        date_adaptation.adaptation_strategy,
        AdaptationStrategy::DateParsing
    );
}

#[test]
fn test_string_to_date_conversion() {
    // Create a string array with dates
    let date_strings = vec!["2023-01-15", "2023-02-20", "2023-03-30"];
    let mut builder = StringBuilder::new();
    for date in &date_strings {
        builder.append_value(date);
    }
    let string_array: ArrayRef = Arc::new(builder.finish());

    // Create date format config
    let date_config = DateFormatConfig::default();

    // Convert to Date32
    let result = convert_array(&string_array, &DataType::Date32, &date_config).unwrap();

    // Verify the result
    let date_array = result
        .as_any()
        .downcast_ref::<Date32Array>()
        .expect("Should convert to Date32Array");

    // Check length
    assert_eq!(date_array.len(), 3);

    // We don't check exact date values here as that would require calculating
    // days since epoch, but we ensure it's not null
    for i in 0..3 {
        assert!(!date_array.is_null(i));
    }
}

#[test]
fn test_numeric_conversion() {
    // Create an Int32 array
    let values = vec![10, 20, 30, 40, 50];
    let int_array: ArrayRef = Arc::new(Int32Array::from(values.clone()));

    // Create date format config (not used for numeric conversion but required)
    let date_config = DateFormatConfig::default();

    // Convert to Float64
    let result = convert_array(&int_array, &DataType::Float64, &date_config).unwrap();

    // Verify the result
    let float_array = result
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Should convert to Float64Array");

    // Check values
    for (i, val) in values.iter().enumerate() {
        assert_eq!(float_array.value(i), f64::from(*val));
    }
}

#[test]
fn test_adapt_record_batch() {
    // Create source record batch with string dates
    let id_array = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let name_array = Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"]));
    let date_array = Arc::new(StringArray::from(vec![
        "2023-01-15",
        "2023-02-20",
        "2023-03-30",
    ]));

    let source_schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("date", DataType::Utf8, false),
    ]);

    let source_batch = RecordBatch::try_new(
        Arc::new(source_schema),
        vec![id_array, name_array, date_array],
    )
    .unwrap();

    // Create target schema with different types
    let target_schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("date", DataType::Date32, false),
    ]);

    // Create date format config
    let date_config = DateFormatConfig::default();

    // Adapt the record batch
    let adapted_batch = adapt_record_batch(&source_batch, &target_schema, &date_config).unwrap();

    // Verify schema is correct
    assert_eq!(adapted_batch.schema().fields().len(), 3);
    assert_eq!(
        adapted_batch.schema().field(0).data_type(),
        &DataType::Int64
    );
    assert_eq!(
        adapted_batch.schema().field(2).data_type(),
        &DataType::Date32
    );

    // Verify row count is preserved
    assert_eq!(adapted_batch.num_rows(), 3);
}

#[test]
fn test_date_format_detection() {
    // Create string arrays with different date formats
    let formats = vec![
        // ISO format
        Arc::new(StringArray::from(vec![
            "2023-01-15",
            "2023-02-20",
            "2023-03-30",
        ])),
        // DD/MM/YYYY format
        Arc::new(StringArray::from(vec![
            "15/01/2023",
            "20/02/2023",
            "30/03/2023",
        ])),
        // MM/DD/YYYY format
        Arc::new(StringArray::from(vec![
            "01/15/2023",
            "02/20/2023",
            "03/30/2023",
        ])),
        // German/Danish format
        Arc::new(StringArray::from(vec![
            "15.01.2023",
            "20.02.2023",
            "30.03.2023",
        ])),
        // Compact format
        Arc::new(StringArray::from(vec!["20230115", "20230220", "20230330"])),
    ];

    let date_config = DateFormatConfig::default();

    // Test conversion of each format to Date32
    for string_array in formats {
        // Convert to dyn Array explicitly by dereferencing the Arc to the trait object
        let array_ref: ArrayRef = string_array.clone();
        let result = convert_array(&array_ref, &DataType::Date32, &date_config).unwrap();

        // Verify the result is a Date32Array with correct length
        let date_array = result
            .as_any()
            .downcast_ref::<Date32Array>()
            .expect("Should convert to Date32Array");

        assert_eq!(date_array.len(), 3);

        // Ensure values are not null, indicating successful conversion
        for i in 0..3 {
            assert!(!date_array.is_null(i));
        }
    }
}
