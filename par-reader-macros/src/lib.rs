//! Procedural macros for the par-reader crate
//!
//! This crate provides procedural macros for generating code from schema
//! definitions, significantly reducing boilerplate in the par-reader crate.

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{parse_macro_input, Attribute, Data, DeriveInput, Fields, Type};

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
///     #[field(name = "PNR", type = "String", nullable = false)]
///     pnr: String,
///
///     #[field(name = "INDUD_KODE", type = "String", nullable = true)]
///     migration_code: Option<String>,
///
///     #[field(name = "HAEND_DATO", type = "Date", nullable = true)]
///     event_date: Option<chrono::NaiveDate>,
/// }
/// ```
#[proc_macro_derive(RegistryTrait, attributes(registry, field))]
pub fn derive_registry_trait(input: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree
    let input = parse_macro_input!(input as DeriveInput);

    // Extract the struct name and fields
    let struct_name = &input.ident;
    let registry_name =
        extract_registry_name(&input.attrs).unwrap_or_else(|| struct_name.to_string());
    let registry_desc =
        extract_registry_desc(&input.attrs).unwrap_or_else(|| format!("{registry_name} registry"));

    // Generate the trait implementation
    let expanded = generate_registry_impl(&input, &registry_name, &registry_desc);

    // Convert back to proc_macro::TokenStream
    TokenStream::from(expanded)
}

/// Extract the registry name from attributes
fn extract_registry_name(attrs: &[Attribute]) -> Option<String> {
    for attr in attrs {
        if attr.path().is_ident("registry") {
            if let Ok(meta) = attr.meta.clone().require_list() {
                // Parse the meta list items
                let nested = meta.tokens.clone();
                // We can't directly access the nested items, but we can convert to a string
                // and check for "name" patterns
                let tokens_str = nested.to_string();
                if let Some(name_pos) = tokens_str.find("name") {
                    // Find the value after "name ="
                    if let Some(value_start) = tokens_str[name_pos..].find('"') {
                        let start_pos = name_pos + value_start + 1;
                        if let Some(value_end) = tokens_str[start_pos..].find('"') {
                            return Some(
                                tokens_str[start_pos..(start_pos + value_end)].to_string(),
                            );
                        }
                    }
                }
            }
        }
    }
    None
}

/// Extract the registry description from attributes
fn extract_registry_desc(attrs: &[Attribute]) -> Option<String> {
    for attr in attrs {
        if attr.path().is_ident("registry") {
            if let Ok(meta) = attr.meta.clone().require_list() {
                // Parse the meta list items
                let nested = meta.tokens.clone();
                // We can't directly access the nested items, but we can convert to a string
                // and check for "description" patterns
                let tokens_str = nested.to_string();
                if let Some(desc_pos) = tokens_str.find("description") {
                    // Find the value after "description ="
                    if let Some(value_start) = tokens_str[desc_pos..].find('"') {
                        let start_pos = desc_pos + value_start + 1;
                        if let Some(value_end) = tokens_str[start_pos..].find('"') {
                            return Some(
                                tokens_str[start_pos..(start_pos + value_end)].to_string(),
                            );
                        }
                    }
                }
            }
        }
    }
    None
}

/// Generate the registry trait implementation
fn generate_registry_impl(
    input: &DeriveInput,
    registry_name: &str,
    registry_desc: &str,
) -> proc_macro2::TokenStream {
    let struct_name = &input.ident;
    let deserializer_name = format_ident!("{}Deserializer", struct_name);

    // Extract the fields from the struct
    let fields = match &input.data {
        Data::Struct(data_struct) => match &data_struct.fields {
            Fields::Named(fields_named) => &fields_named.named,
            _ => panic!("Only named fields are supported"),
        },
        _ => panic!("Only structs are supported"),
    };

    // Generate field mappings
    let field_mappings = fields.iter().map(|field| {
        let field_name = field.ident.as_ref().unwrap();
        let field_type = &field.ty;

        // Extract field attributes
        let source_name = extract_field_name(&field.attrs)
            .unwrap_or_else(|| field_name.to_string().to_uppercase());
        let nullable = is_option_type(field_type);
        let (field_type_enum, extractor_method, setter_method) = extract_field_type_info(field_type);

        quote! {
            FieldMapping::new(
                FieldDefinition::new(
                    #source_name,
                    stringify!(#field_name),
                    FieldType::#field_type_enum,
                    #nullable,
                ),
                Extractors::#extractor_method(#source_name),
                ModelSetters::#setter_method(|individual, value| {
                    individual.#field_name = value;
                }),
            )
        }
    });

    // Generate the trait implementation
    quote! {
        /// Auto-generated deserializer for #registry_name registry
        #[derive(Debug)]
        pub struct #deserializer_name {
            inner: std::sync::Arc<dyn crate::registry::trait_deserializer::RegistryDeserializer + Send + Sync>,
        }

        impl #deserializer_name {
            /// Create a new deserializer for #registry_name registry
            #[must_use]
            pub fn new() -> Self {
                // Create the schema
                let schema = Self::create_schema();

                // Create the deserializer
                let inner = std::sync::Arc::new(
                    crate::generate_trait_deserializer!(
                        #registry_name,
                        #registry_desc,
                        || schema
                    )
                );

                Self { inner }
            }

            /// Create the schema for #registry_name registry
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
                -> crate::error::Result<Vec<crate::models::core::Individual>> {
                self.inner.deserialize_batch(batch)
            }

            /// Deserialize a single row from a record batch using this deserializer
            pub fn deserialize_row(&self, batch: &arrow::record_batch::RecordBatch, row: usize)
                -> crate::error::Result<Option<crate::models::core::Individual>> {
                self.inner.deserialize_row(batch, row)
            }
        }

        impl Default for #deserializer_name {
            fn default() -> Self {
                Self::new()
            }
        }
    }
}

/// Extract the field name from attributes
fn extract_field_name(attrs: &[Attribute]) -> Option<String> {
    for attr in attrs {
        if attr.path().is_ident("field") {
            if let Ok(meta) = attr.meta.clone().require_list() {
                // Parse the meta list items
                let nested = meta.tokens.clone();
                // We can't directly access the nested items, but we can convert to a string
                // and check for "name" patterns
                let tokens_str = nested.to_string();
                if let Some(name_pos) = tokens_str.find("name") {
                    // Find the value after "name ="
                    if let Some(value_start) = tokens_str[name_pos..].find('"') {
                        let start_pos = name_pos + value_start + 1;
                        if let Some(value_end) = tokens_str[start_pos..].find('"') {
                            return Some(
                                tokens_str[start_pos..(start_pos + value_end)].to_string(),
                            );
                        }
                    }
                }
            }
        }
    }
    None
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
fn extract_field_type_info(ty: &Type) -> (proc_macro2::TokenStream, proc_macro2::TokenStream, proc_macro2::TokenStream) {
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
    (quote! { String }, quote! { string }, quote! { string_setter })
}

/// Extract the inner type name, extractor method, and setter method
fn extract_inner_type_info(ty: &Type) -> (proc_macro2::TokenStream, proc_macro2::TokenStream, proc_macro2::TokenStream) {
    if let Type::Path(type_path) = ty {
        let path = &type_path.path;
        if path.segments.len() == 1 {
            let segment = &path.segments[0];
            let ident = &segment.ident;

            if ident == "String" || ident == "str" {
                return (quote! { String }, quote! { string }, quote! { string_setter });
            } else if ident == "i8"
                || ident == "i16"
                || ident == "i32"
                || ident == "i64"
                || ident == "u8"
                || ident == "u16"
                || ident == "u32"
                || ident == "u64"
            {
                return (quote! { Integer }, quote! { integer }, quote! { i32_setter });
            } else if ident == "f32" || ident == "f64" {
                return (quote! { Decimal }, quote! { decimal }, quote! { f64_setter });
            } else if ident == "NaiveDate" || ident == "Date" {
                return (quote! { Date }, quote! { date }, quote! { date_setter });
            }
        }
    }

    // Default to String if type can't be determined
    (quote! { String }, quote! { string }, quote! { string_setter })
}
