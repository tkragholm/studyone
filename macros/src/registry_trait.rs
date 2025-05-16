//! RegistryTrait derive macro implementation
//!
//! This module contains the implementation of the RegistryTrait derive macro,
//! which is used to generate code for registry trait implementations.

use darling::{ast, FromDeriveInput, FromField};
use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{parse_macro_input, DeriveInput, Type};

use crate::utils;

/// Receiver for the struct that derives `RegistryTrait`
#[derive(Debug, FromDeriveInput)]
#[darling(attributes(registry), supports(struct_named))]
struct RegistryTraitReceiver {
    /// The struct identifier
    ident: syn::Ident,
    /// Registry options from the #[registry(...)] attribute
    #[darling(default)]
    name: Option<String>,
    #[darling(default)]
    description: Option<String>,
    /// Identifier field type (pnr, `record_number`, or `dw_ek_kontakt`)
    #[darling(default)]
    id_field: Option<String>,
    /// The struct data with parsed fields
    data: ast::Data<(), RegistryFieldReceiver>,
}

/// Receiver for the fields in the struct
#[derive(Debug, FromField)]
#[darling(attributes(field))]
struct RegistryFieldReceiver {
    /// The field identifier
    ident: Option<syn::Ident>,
    /// The field type
    ty: syn::Type,
    /// Field name attribute
    #[darling(default, rename = "name")]
    field_name: Option<String>,
    /// Field nullability attribute
    /// Currently unused, but will be used in the future to determine if a field can be null
    #[darling(default)]
    #[allow(dead_code)]
    nullable: Option<bool>,
}

/// Process the RegistryTrait derive macro
pub fn process_derive_registry_trait(input: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree
    let input = parse_macro_input!(input as DeriveInput);

    // Parse with darling
    let receiver = match RegistryTraitReceiver::from_derive_input(&input) {
        Ok(receiver) => receiver,
        Err(err) => return err.write_errors().into(),
    };

    // Generate the trait implementation
    let struct_name = &receiver.ident;
    let registry_name = receiver
        .name
        .clone()
        .unwrap_or_else(|| struct_name.to_string());
    let registry_desc = receiver
        .description
        .clone()
        .unwrap_or_else(|| format!("{registry_name} registry"));

    // Get the ID field type (default to "pnr" if not specified)
    let id_field = receiver
        .id_field
        .clone()
        .unwrap_or_else(|| "pnr".to_string());

    // Extract the fields
    let ast::Data::Struct(fields) = &receiver.data else {
        unreachable!("Darling ensures this is a struct")
    };

    // Generate the trait implementation
    let expanded = generate_registry_impl(
        &receiver.ident,
        &registry_name,
        &registry_desc,
        &id_field,
        fields,
    );

    // Convert back to proc_macro::TokenStream
    TokenStream::from(expanded)
}

/// Generate the registry trait implementation
fn generate_registry_impl(
    struct_name: &syn::Ident,
    registry_name: &str,
    registry_desc: &str,
    id_field: &str,
    fields: &ast::Fields<RegistryFieldReceiver>,
) -> proc_macro2::TokenStream {
    let deserializer_name = format_ident!("{}Deserializer", struct_name);

    // Extract field names for use in impl blocks
    let field_names: Vec<_> = fields
        .iter()
        .map(|field| field.ident.as_ref().unwrap())
        .collect();

    // Generate field mappings
    let field_mappings = fields.iter().map(|field| {
        let field_name = field.ident.as_ref().unwrap();
        let field_name_str = field_name.to_string();
        let field_type = &field.ty;

        // Extract field attributes
        let source_name = field
            .field_name
            .clone()
            .unwrap_or_else(|| field_name.to_string().to_uppercase());

        // Determine the target property name for ID fields
        // We need to make sure it's mapped to the standardized name expected by the trait deserializer
        let target_property_name = if field_name_str == "record_number" {
            // For RECNUM fields, always use "record_number" as the target property name
            "record_number".to_string()
        } else if field_name_str == "dw_ek_kontakt" {
            // For DW_EK_KONTAKT fields, always use "dw_ek_kontakt" as the target property name
            "dw_ek_kontakt".to_string()
        } else {
            // For other fields, use the field name as-is
            field_name.to_string()
        };

        let is_option = utils::is_option_type(field_type);
        let (field_type_enum, extractor_method, setter_method) =
            utils::extract_field_type_info(field_type);

        // Generate the setter code based on field type and whether it's the ID field
        let setter_code = utils::generate_field_setter_code(field_name, field_type, id_field);

        quote! {
            crate::schema::field_def::FieldMapping::new(
                crate::schema::field_def::FieldDefinition::new(
                    #source_name,
                    #target_property_name,
                    crate::schema::field_def::FieldType::#field_type_enum,
                    #is_option,
                ),
                crate::schema::field_def::mapping::Extractors::#extractor_method(#source_name),
                crate::schema::field_def::mapping::ModelSetters::#setter_method(#setter_code),
            )
        }
    });

    // Check if the struct has the specified ID field
    let has_id_field = fields
        .iter()
        .any(|field| field.ident.as_ref().is_some_and(|ident| *ident == id_field));

    // Check if the struct has a record_number field (for debugging)
    let _has_record_number = has_field_named(fields, "record_number");

    // Update the unused variable to avoid warnings
    let _has_id_field = has_id_field;

    // Prepare field extraction for properties
    let field_extraction_statements = fields.iter().filter_map(|field| {
        let field_name = field.ident.as_ref().unwrap();
        let field_name_str = field_name.to_string();

        // Skip ID field extraction as it's handled separately
        if field_name_str == id_field {
            None
        } else {
            // Extract field type information before generating code
            let is_option = utils::is_option_type(&field.ty);

            // For non-ID fields, generate property extraction code based on the field type
            // First get the field type as a string for matching
            let field_type = if let Type::Path(type_path) = &field.ty {
                if let Some(segment) = type_path.path.segments.first() {
                    segment.ident.to_string()
                } else {
                    "Unknown".to_string()
                }
            } else {
                "Unknown".to_string()
            };

            // For Option types, extract the inner type
            let inner_type = if field_type == "Option" {
                if let Type::Path(type_path) = &field.ty {
                    if let Some(segment) = type_path.path.segments.first() {
                        if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                            if let Some(syn::GenericArgument::Type(Type::Path(inner_path))) = args.args.first() {
                                if let Some(inner_segment) = inner_path.path.segments.first() {
                                    inner_segment.ident.to_string()
                                } else {
                                    "Unknown".to_string()
                                }
                            } else {
                                "Unknown".to_string()
                            }
                        } else {
                            "Unknown".to_string()
                        }
                    } else {
                        "Unknown".to_string()
                    }
                } else {
                    "Unknown".to_string()
                }
            } else {
                field_type
            };

            // Now match on the type for code generation
            match inner_type.as_str() {
                // For String fields
                "String" => {
                    if is_option {
                        // For Option<String> fields
                        Some(quote! {
                            // Extract Option<String> property - try both field name and source field name
                            if let Some(props) = individual.properties() {
                                // Debug logging has been removed

                                // Try both actual field name and stringified field name
                                let property_value = props.get(#field_name_str)
                                    .or_else(|| props.get(stringify!(#field_name)));

                                if let Some(value) = property_value {
                                    if let Some(string_val) = value.downcast_ref::<Option<String>>() {
                                        // Debug logging has been removed
                                        instance.#field_name = string_val.clone();
                                    }
                                }
                            }
                        })
                    } else {
                        // For String fields (non-Option)
                        Some(quote! {
                            // Extract String property - try both field name and source field name
                            if let Some(props) = individual.properties() {
                                // Try both actual field name and stringified field name
                                let property_value = props.get(#field_name_str)
                                    .or_else(|| props.get(stringify!(#field_name)));

                                if let Some(value) = property_value {
                                    if let Some(string_val) = value.downcast_ref::<String>() {
                                        instance.#field_name = string_val.clone();
                                    }
                                }
                            }
                        })
                    }
                },
                // For Date fields
                "Date" | "NaiveDate" => {
                    Some(quote! {
                        // Extract Option<NaiveDate> property - try both field name and source field name
                        if let Some(props) = individual.properties() {
                            // Debug logging has been removed

                            // Try both actual field name and stringified field name
                            let property_value = props.get(#field_name_str)
                                .or_else(|| props.get(stringify!(#field_name)));

                            if let Some(value) = property_value {
                                if let Some(date_val) = value.downcast_ref::<Option<chrono::NaiveDate>>() {
                                    // Debug logging has been removed
                                    instance.#field_name = *date_val;
                                }
                            }
                        }
                    })
                },
                // For Integer and other field types - add more as needed
                _ => {
                    // Default extraction for other types
                    Some(quote! {
                        // Extract generic property
                        if let Some(props) = individual.properties() {
                            if let Some(_value) = props.get(#field_name_str) {
                                // No specific handling for this type yet
                            }
                        }
                    })
                }
            }
        }
    }).collect::<Vec<_>>();

    // Generate appropriate From implementation
    let from_impl = if id_field == "pnr" {
        // For structs using PNR as id_field
        quote! {
            impl From<crate::models::core::Individual> for #struct_name {
                fn from(individual: crate::models::core::Individual) -> Self {
                    // Create a default instance of our struct
                    let mut instance = Self::default();

                    // Set the PNR field directly
                    instance.pnr = individual.pnr.clone();

                    // Extract all other fields from properties
                    #(#field_extraction_statements)*

                    // Return the populated instance
                    instance
                }
            }
        }
    } else if id_field == "record_number" {
        // For structs using record_number as id_field (LPR_DIAG)
        quote! {
            impl From<crate::models::core::Individual> for #struct_name {
                fn from(individual: crate::models::core::Individual) -> Self {
                    // Create a default instance of our struct
                    let mut instance = Self::default();

                    // Extract record_number and all other fields from properties
                    if let Some(props) = individual.properties() {
                        if let Some(record_num) = props.get("record_number") {
                            if let Some(recnum) = record_num.downcast_ref::<Option<String>>() {
                                instance.record_number = recnum.clone();
                            }
                        }
                    }

                    // Extract all other fields from properties
                    #(#field_extraction_statements)*

                    // Return the populated instance
                    instance
                }
            }
        }
    } else if id_field == "dw_ek_kontakt" {
        // For structs using dw_ek_kontakt as id_field (LPR3)
        quote! {
            impl From<crate::models::core::Individual> for #struct_name {
                fn from(individual: crate::models::core::Individual) -> Self {
                    // Create a default instance of our struct
                    let mut instance = Self::default();

                    // Extract dw_ek_kontakt and all other fields from properties
                    if let Some(props) = individual.properties() {
                        if let Some(kontakt) = props.get("dw_ek_kontakt") {
                            if let Some(dw_ek_kontakt) = kontakt.downcast_ref::<Option<String>>() {
                                instance.dw_ek_kontakt = dw_ek_kontakt.clone();
                            }
                        }
                    }

                    // Extract all other fields from properties
                    #(#field_extraction_statements)*

                    // Return the populated instance
                    instance
                }
            }
        }
    } else {
        // Default implementation for other id_field types
        quote! {
            impl From<crate::models::core::Individual> for #struct_name {
                fn from(individual: crate::models::core::Individual) -> Self {

                    // Create a default instance of our struct
                    let mut instance = Self::default();

                    // Extract all fields from properties
                    #(#field_extraction_statements)*

                    // Return the populated instance
                    instance
                }
            }
        }
    };

    // Generate the complete implementation
    quote! {
        /// Auto-generated deserializer for registry
        ///
        /// This deserializer was generated by the RegistryTrait derive macro
        /// and provides methods to deserialize Arrow record batches into Individual models.
        #[derive(Debug)]
        pub struct #deserializer_name {
            inner: std::sync::Arc<dyn crate::registry::trait_deserializer::RegistryDeserializer>,
        }

        // Implement the registry type trait for our struct to make types compatible
        impl crate::registry::trait_deserializer::RegistryType for #struct_name {}

        impl #deserializer_name {
            /// Create a new deserializer for registry
            #[must_use]
            pub fn new() -> Self {
                // Create the schema
                let schema = Self::create_schema();

                // Create the deserializer implementation
                let inner = std::sync::Arc::new(
                    crate::registry::trait_deserializer_impl::RegistryDeserializerImpl::new(
                        #registry_name,
                        #registry_desc,
                        schema,
                        Some(#id_field)
                    )
                );

                Self { inner }
            }

            /// Create the schema definition for this registry
            fn create_schema() -> crate::schema::RegistrySchema {
                // Create field mappings
                let field_mappings = vec![
                    #(#field_mappings),*
                ];

                crate::schema::create_registry_schema(
                    #registry_name,
                    #registry_desc,
                    field_mappings
                )
            }

            /// Deserialize a record batch using this deserializer
            pub fn deserialize_batch(&self, batch: &arrow::record_batch::RecordBatch)
                -> crate::error::Result<Vec<#struct_name>> {
                // Convert from Individual to our specific type
                let result = self.inner.deserialize_batch(batch)?
                    .into_iter()
                    .map(|individual| #struct_name::from(individual))
                    .collect();
                Ok(result)
            }

            /// Deserialize a single row from a record batch using this deserializer
            pub fn deserialize_row(&self, batch: &arrow::record_batch::RecordBatch, row: usize)
                -> crate::error::Result<Option<#struct_name>> {
                // Convert from Individual to our specific type
                let result = self.inner.deserialize_row(batch, row)?
                    .map(|individual| #struct_name::from(individual));
                Ok(result)
            }
        }

        // From implementation
        #from_impl

        // Implement Default for our struct
        impl Default for #struct_name {
            fn default() -> Self {
                Self {
                    #(#field_names: Default::default()),*
                }
            }
        }

        impl Default for #deserializer_name {
            fn default() -> Self {
                Self::new()
            }
        }
    }
}

/// Helper to check if a struct has a specific field
fn has_field_named(fields: &ast::Fields<RegistryFieldReceiver>, name: &str) -> bool {
    fields
        .iter()
        .any(|field| field.ident.as_ref().is_some_and(|ident| *ident == name))
}