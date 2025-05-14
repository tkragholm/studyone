//! Procedural macros for the par-reader crate
//!
//! This crate provides procedural macros for generating code from schema
//! definitions, significantly reducing boilerplate in the par-reader crate.

use darling::{ast, FromDeriveInput, FromField};
use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{parse_macro_input, DeriveInput, Type};

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

/// Derive macro for generating registry traits
///
/// This macro generates a registry trait implementation from a struct definition.
///
/// # Example
///
/// ```rust
/// #[derive(RegistryTrait)]
/// #[registry(name = "VNDS", description = "Migration registry")]
/// struct VndsRegistry {
///     #[field(name = "PNR")]
///     pnr: String,
///
///     #[field(name = "INDUD_KODE")]
///     migration_code: Option<String>,
///
///     #[field(name = "HAEND_DATO")]
///     event_date: Option<chrono::NaiveDate>,
/// }
/// ```
#[proc_macro_derive(RegistryTrait, attributes(registry, field))]
pub fn derive_registry_trait(input: TokenStream) -> TokenStream {
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

    // Extract the fields
    let ast::Data::Struct(fields) = &receiver.data else {
        unreachable!("Darling ensures this is a struct")
    };

    // Generate the trait implementation
    let expanded = generate_registry_impl(&receiver.ident, &registry_name, &registry_desc, fields);

    // Convert back to proc_macro::TokenStream
    TokenStream::from(expanded)
}

/// Generate the registry trait implementation
fn generate_registry_impl(
    struct_name: &syn::Ident,
    registry_name: &str,
    registry_desc: &str,
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
        let field_type = &field.ty;

        // Extract field attributes
        let source_name = field.field_name.clone()
            .unwrap_or_else(|| field_name.to_string().to_uppercase());

        let is_option = is_option_type(field_type);
        let (field_type_enum, extractor_method, setter_method) = extract_field_type_info(field_type);

        // Generate setter code
        // We'll use the set_property method in Individual to store values by field name
        let is_option_field = is_option_type(field_type);
        
        // Generate setter code based on the field type
        let field_name_str = field_name.to_string();
        let setter_code = match field_name_str.as_str() {
            // Special handling for date fields
            "event_date" => {
                quote! {
                    |individual, value| {
                        use chrono::NaiveDate;
                        use std::any::Any;
                        
                        // Debug logging
                        static mut SETTER_COUNT: usize = 0;
                        unsafe {
                            if SETTER_COUNT < 5 {
                                println!("Setting event_date value to Individual: value={:?}", value);
                                SETTER_COUNT += 1;
                            }
                        }
                        
                        // Cast to Individual
                        let individual_obj = individual as &mut crate::models::core::Individual;
                        
                        // Convert the value to Option<NaiveDate> and box it
                        let boxed_value: Box<dyn Any> = Box::new(Some(value as NaiveDate));
                        individual_obj.set_property("event_date", boxed_value);
                    }
                }
            },
            // Generic handling for other Option<T> fields
            _ if is_option_field => {
                quote! {
                    |individual, value| {
                        // Add debug logging for the first few calls
                        static mut SETTER_COUNT: usize = 0;
                        unsafe {
                            if SETTER_COUNT < 5 {
                                println!("Setting Optional {} value to Individual: field={}, value={:?}", 
                                        stringify!(#field_name), stringify!(#field_name), value);
                                SETTER_COUNT += 1;
                            }
                        }
                        
                        // Since Individual is the concrete type we're working with in our trait implementation,
                        // we can simply cast it directly. The individual parameter is a &mut dyn Any, which we
                        // know is really a &mut Individual.
                        let individual_obj = individual as &mut crate::models::core::Individual;
                        
                        // For Option<T> fields, we need to wrap the value in Some
                        // Box::new can't directly box None, so we need to create an Option first
                        let boxed_value: Box<dyn std::any::Any> = Box::new(Some(value));
                        individual_obj.set_property(stringify!(#field_name), boxed_value);
                    }
                }
            },
            // Generic handling for non-Option fields
            _ => {
                quote! {
                    |individual, value| {
                        // Add debug logging for the first few calls
                        static mut SETTER_COUNT: usize = 0;
                        unsafe {
                            if SETTER_COUNT < 5 {
                                println!("Setting {} value to Individual: field={}, value={:?}", 
                                        stringify!(#field_name), stringify!(#field_name), value);
                                SETTER_COUNT += 1;
                            }
                        }
                        
                        // Since Individual is the concrete type we're working with in our trait implementation,
                        // we can simply cast it directly. The individual parameter is a &mut dyn Any, which we
                        // know is really a &mut Individual.
                        let individual_obj = individual as &mut crate::models::core::Individual;
                        
                        // Box the value directly for non-Option fields
                        individual_obj.set_property(stringify!(#field_name), Box::new(value));
                    }
                }
            }
        };

        quote! {
            crate::schema::field_def::FieldMapping::new(
                crate::schema::field_def::FieldDefinition::new(
                    #source_name,
                    stringify!(#field_name),
                    crate::schema::field_def::FieldType::#field_type_enum,
                    #is_option,
                ),
                crate::schema::field_def::mapping::Extractors::#extractor_method(#source_name),
                crate::schema::field_def::mapping::ModelSetters::#setter_method(#setter_code),
            )
        }
    });

    // Generate the trait implementation
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
                        schema
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

        // Comment out the From implementation for now
        // We'll focus on getting the basic deserializer working first
        // This avoids complex type mappings between Individual and custom types

        // The example will need to be updated to use the Individual directly
        impl From<crate::models::core::Individual> for #struct_name {
            fn from(individual: crate::models::core::Individual) -> Self {
                // Only print for the first few records to avoid flooding the console
                static mut PRINT_COUNT: usize = 0;
                unsafe {
                    if PRINT_COUNT < 3 {
                        println!("Converting Individual: PNR='{}'", individual.pnr);
                        PRINT_COUNT += 1;
                    }
                }
                
                // Create a default instance of our struct
                let mut instance = Self::default();
                
                // Set the PNR field which is guaranteed to exist
                instance.pnr = individual.pnr;
                
                // Return the populated instance
                instance
            }
        }

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

/// Check if a type is an Option<T>
fn is_option_type(ty: &Type) -> bool {
    match ty {
        Type::Path(type_path) => {
            let path = &type_path.path;
            if path.segments.len() == 1 {
                let segment = &path.segments[0];
                segment.ident == "Option"
            } else {
                false
            }
        }
        _ => false,
    }
}

/// Extract the field type, extractor method, and setter method from a Type
fn extract_field_type_info(
    ty: &Type,
) -> (
    proc_macro2::TokenStream,
    proc_macro2::TokenStream,
    proc_macro2::TokenStream,
) {
    if let Type::Path(type_path) = ty {
        let path = &type_path.path;
        if path.segments.len() == 1 {
            let segment = &path.segments[0];
            let ident = &segment.ident;

            // Extract the inner type if it's an Option<T>
            if ident == "Option" {
                if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                    if let Some(syn::GenericArgument::Type(inner_type)) = args.args.first() {
                        return extract_inner_type_info(inner_type);
                    }
                }
            }

            return extract_inner_type_info(ty);
        }
    }

    // Default to String if type can't be determined
    (
        quote! { String },
        quote! { string },
        quote! { string_setter },
    )
}

/// Extract the inner type name, extractor method, and setter method
fn extract_inner_type_info(
    ty: &Type,
) -> (
    proc_macro2::TokenStream,
    proc_macro2::TokenStream,
    proc_macro2::TokenStream,
) {
    if let Type::Path(type_path) = ty {
        let path = &type_path.path;
        if path.segments.len() == 1 {
            let segment = &path.segments[0];
            let ident = &segment.ident;

            if ident == "String" || ident == "str" {
                return (
                    quote! { String },
                    quote! { string },
                    quote! { string_setter },
                );
            } else if ident == "i8"
                || ident == "i16"
                || ident == "i32"
                || ident == "i64"
                || ident == "u8"
                || ident == "u16"
                || ident == "u32"
                || ident == "u64"
            {
                return (
                    quote! { Integer },
                    quote! { integer },
                    quote! { i32_setter },
                );
            } else if ident == "f32" || ident == "f64" {
                return (
                    quote! { Decimal },
                    quote! { decimal },
                    quote! { f64_setter },
                );
            } else if ident == "NaiveDate" || ident == "Date" {
                return (quote! { Date }, quote! { date }, quote! { date_setter });
            } else if ident == "bool" {
                return (
                    quote! { Boolean },
                    quote! { boolean },
                    quote! { bool_setter },
                );
            }
        }
    }

    // Default to String if type can't be determined
    (
        quote! { String },
        quote! { string },
        quote! { string_setter },
    )
}

/// Get the Rust type for a field
#[allow(dead_code)]
fn get_rust_type(ty: &Type) -> proc_macro2::TokenStream {
    if let Type::Path(type_path) = ty {
        let path = &type_path.path;
        if path.segments.len() == 1 {
            let segment = &path.segments[0];
            let ident = &segment.ident;

            // Check if it's an Option type to extract inner type
            if ident == "Option" {
                if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                    if let Some(syn::GenericArgument::Type(inner_type)) = args.args.first() {
                        return get_rust_type(inner_type);
                    }
                }
            }

            // Return the actual type
            if ident == "String" || ident == "str" {
                return quote! { String };
            } else if ident == "i8"
                || ident == "i16"
                || ident == "i32"
                || ident == "i64"
                || ident == "u8"
                || ident == "u16"
                || ident == "u32"
                || ident == "u64"
            {
                return quote! { i32 };
            } else if ident == "f32" || ident == "f64" {
                return quote! { f64 };
            } else if ident == "NaiveDate" || ident == "Date" {
                return quote! { chrono::NaiveDate };
            } else if ident == "bool" {
                return quote! { bool };
            }

            // If it's another type, just use its identifier
            return quote! { #ident };
        }
    }

    // Default to String if type can't be determined
    quote! { String }
}
