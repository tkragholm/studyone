//! Macros for field definitions and mappings
//!
//! This module provides macros to reduce boilerplate when defining registry schemas.

/// Macro to create a registry schema with field mappings
///
/// This macro makes it easier to define a registry schema with field mappings.
///
/// # Example
///
/// ```rust
/// use crate::schema::registry_schema;
/// use crate::models::core::types::SocioeconomicStatus;
///
/// let akm_schema = registry_schema! {
///     name: "AKM",
///     description: "Employment information registry",
///     fields: [
///         // Basic identification
///         {
///             name: "PNR",
///             type: PNR,
///             required: true,
///             map_to: |individual, value: String| individual.set_pnr(value)
///         },
///         // Socioeconomic status
///         {
///             name: "SOCIO",
///             type: Category,
///             required: false,
///             map_to: |individual, value: i32| {
///                 let status = match value {
///                     110..=129 => SocioeconomicStatus::SelfEmployedWithEmployees,
///                     _ => SocioeconomicStatus::Unknown,
///                 };
///                 individual.set_socioeconomic_status(status);
///             }
///         }
///     ]
/// };
/// ```
#[macro_export]
macro_rules! registry_schema {
    (
        name: $name:expr,
        description: $description:expr,
        fields: [
            $(
                {
                    name: $field_name:expr,
                    type: $field_type:ident,
                    required: $required:expr,
                    $(description: $field_description:expr,)?
                    $(map_to: $mapper:expr $(,)?)?
                }
            ),* $(,)?
        ]
    ) => {
        {
            use $crate::schema::{FieldDefinition, FieldType, FieldMapping, ModelSetters, Extractors, create_registry_schema};

            let field_mappings = vec![
                $(
                    {
                        let field_def = FieldDefinition::new(
                            $field_name,
                            $($field_description,)?
                            $(stringify!($field_description),)?
                            FieldType::$field_type,
                            !$required,
                        );

                        let extractor = match FieldType::$field_type {
                            FieldType::PNR | FieldType::String | FieldType::Other => Extractors::string($field_name),
                            FieldType::Integer | FieldType::Category => Extractors::integer($field_name),
                            FieldType::Decimal => Extractors::decimal($field_name),
                            FieldType::Boolean => Extractors::boolean($field_name),
                            FieldType::Date => Extractors::date($field_name),
                        };

                        $(
                            let setter = match FieldType::$field_type {
                                FieldType::PNR | FieldType::String | FieldType::Other => {
                                    ModelSetters::string_setter($mapper)
                                },
                                FieldType::Integer | FieldType::Category => {
                                    ModelSetters::i32_setter($mapper)
                                },
                                FieldType::Decimal => {
                                    ModelSetters::f64_setter($mapper)
                                },
                                FieldType::Boolean => {
                                    ModelSetters::bool_setter($mapper)
                                },
                                FieldType::Date => {
                                    ModelSetters::date_setter($mapper)
                                },
                            };
                        )?

                        $(
                            FieldMapping::new(field_def, extractor, setter)
                        )?
                        $(
                            // If no mapper is provided, create a dummy one that does nothing
                            FieldMapping::new(
                                field_def,
                                extractor,
                                ModelSetters::string_setter(|_, _| {})
                            )
                        )?
                    }
                ),*
            ];

            create_registry_schema($name, $description, field_mappings)
        }
    };
}

/// Macro to create a field mapping
///
/// This macro makes it easier to define a field mapping.
///
/// # Example
///
/// ```rust
/// use crate::schema::field_mapping;
/// use crate::models::core::types::Gender;
///
/// let pnr_mapping = field_mapping! {
///     name: "PNR",
///     description: "Personal identification number",
///     type: PNR,
///     required: true,
///     map_to: |individual, value: String| individual.set_pnr(value)
/// };
///
/// let gender_mapping = field_mapping! {
///     name: "KOEN",
///     description: "Gender",
///     type: Category,
///     required: false,
///     map_to: |individual, value: String| {
///         individual.set_gender(Gender::from(value.as_str()));
///     }
/// };
/// ```
#[macro_export]
macro_rules! field_mapping {
    (
        name: $name:expr,
        description: $description:expr,
        type: $type:ident,
        required: $required:expr,
        map_to: $mapper:expr
    ) => {
        {
            use $crate::schema::{FieldDefinition, FieldType, FieldMapping, ModelSetters, Extractors};

            let field_def = FieldDefinition::new(
                $name,
                $description,
                FieldType::$type,
                !$required,
            );

            let extractor = match FieldType::$type {
                FieldType::PNR | FieldType::String | FieldType::Other => Extractors::string($name),
                FieldType::Integer | FieldType::Category => Extractors::integer($name),
                FieldType::Decimal => Extractors::decimal($name),
                FieldType::Boolean => Extractors::boolean($name),
                FieldType::Date => Extractors::date($name),
            };

            let setter = match FieldType::$type {
                FieldType::PNR | FieldType::String | FieldType::Other => {
                    ModelSetters::string_setter($mapper)
                },
                FieldType::Integer | FieldType::Category => {
                    ModelSetters::i32_setter($mapper)
                },
                FieldType::Decimal => {
                    ModelSetters::f64_setter($mapper)
                },
                FieldType::Boolean => {
                    ModelSetters::bool_setter($mapper)
                },
                FieldType::Date => {
                    ModelSetters::date_setter($mapper)
                },
            };

            FieldMapping::new(field_def, extractor, setter)
        }
    };
}