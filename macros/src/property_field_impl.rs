//! PropertyField derive macro implementation
//!
//! This module contains the implementation of the PropertyField derive macro,
//! which is used to generate property reflection code for struct fields.

use darling::{ast, FromDeriveInput, FromField};
use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput, Type};

/// Receiver for the struct that derives PropertyField
#[derive(Debug, FromDeriveInput)]
#[darling(attributes(property), supports(struct_named))]
pub struct PropertyFieldReceiver {
    /// The struct identifier
    ident: syn::Ident,
    /// The struct data with parsed fields
    data: ast::Data<(), PropertyFieldFieldReceiver>,
}

/// Receiver for the fields in the struct
#[derive(Debug, FromField)]
#[darling(attributes(property))]
pub struct PropertyFieldFieldReceiver {
    /// The field identifier
    ident: Option<syn::Ident>,
    /// The field type
    ty: syn::Type,
    /// Property name attribute
    #[darling(default, rename = "name")]
    property_name: Option<String>,
    /// Registry type the field belongs to
    #[darling(default)]
    #[allow(dead_code)]
    registry: Option<String>,
    /// Whether the field is nullable
    #[darling(default)]
    #[allow(dead_code)]
    nullable: Option<bool>,
    /// Description of the field
    #[darling(default)]
    #[allow(dead_code)]
    description: Option<String>,
}

/// Process the PropertyField derive macro
pub fn process_derive_property_field(input: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree
    let input = parse_macro_input!(input as DeriveInput);

    // Parse with darling
    let receiver = match PropertyFieldReceiver::from_derive_input(&input) {
        Ok(receiver) => receiver,
        Err(err) => return err.write_errors().into(),
    };

    // Extract the fields
    let ast::Data::Struct(fields) = &receiver.data else {
        unreachable!("Darling ensures this is a struct")
    };

    // Generate the property reflection code
    let struct_name = &receiver.ident;
    let expanded = generate_property_field_impl(struct_name, fields);

    // Convert back to proc_macro::TokenStream
    TokenStream::from(expanded)
}

/// Generate the property field implementation
fn generate_property_field_impl(
    struct_name: &syn::Ident, 
    fields: &ast::Fields<PropertyFieldFieldReceiver>,
) -> proc_macro2::TokenStream {
    // Extract field information
    let property_field_setters = fields.iter().filter_map(|field| {
        let field_name = field.ident.as_ref()?;
        let field_name_str = field_name.to_string();
        
        // Get property name from attribute or use field name
        let property_name = field.property_name.clone()
            .unwrap_or_else(|| field_name.to_string());
        
        // Get field type information
        let field_type = &field.ty;
        
        // Generate setter code based on field type
        let setter_code = generate_field_setter_code(field_name, field_type, &field_name_str);
        
        Some(quote! {
            // Setter implementation
            if property == #property_name {
                #setter_code
                return;
            }
        })
    }).collect::<Vec<_>>();
    
    // Generate the property field implementation
    quote! {
        impl #struct_name {
            /// Set a property value
            pub fn set_property_field(&mut self, property: &str, value: Box<dyn std::any::Any + Send + Sync>) {
                // Try to set the value in the appropriate field
                #(#property_field_setters)*
                
                // If not handled above, store it in the properties map
                // This assumes the struct has a properties field or method
                if let Some(props) = &mut self.properties {
                    props.insert(property.to_string(), value);
                }
            }
        }
    }
}

/// Check if a type is an Option<T>
fn is_option_type(ty: &Type) -> bool {
    if let Type::Path(type_path) = ty {
        if let Some(segment) = type_path.path.segments.first() {
            if segment.ident == "Option" {
                return true;
            }
        }
    }
    false
}

/// Generate field setter code
fn generate_field_setter_code(field_name: &syn::Ident, field_type: &Type, _field_name_str: &str) -> proc_macro2::TokenStream {
    if is_option_type(field_type) {
        // Extract inner type
        let inner_type = extract_inner_option_type(field_type);
        
        match inner_type.as_str() {
            "String" => quote! {
                if let Some(v) = value.downcast_ref::<Option<String>>() {
                    println!("Setting Optional string value to Individual: field={}, value={:?}", stringify!(#field_name), v);
                    self.#field_name = v.clone();
                } else if let Some(v) = value.downcast_ref::<String>() {
                    // Also try non-Option String for flexibility
                    println!("Setting string value to Optional<String> field Individual: field={}, value={}", stringify!(#field_name), v);
                    self.#field_name = Some(v.clone());
                }
            },
            "NaiveDate" => quote! {
                if let Some(v) = value.downcast_ref::<Option<chrono::NaiveDate>>() {
                    println!("Setting Optional date value to Individual: field={}, value={:?}", stringify!(#field_name), v);
                    self.#field_name = *v;
                } else if let Some(v) = value.downcast_ref::<chrono::NaiveDate>() {
                    // Also try non-Option Date for flexibility
                    println!("Setting date value to Optional<NaiveDate> field: field={}", stringify!(#field_name));
                    self.#field_name = Some(*v);
                }
            },
            "i32" => quote! {
                if let Some(v) = value.downcast_ref::<Option<i32>>() {
                    println!("Setting Optional i32 value to Individual: field={}, value={:?}", stringify!(#field_name), v);
                    self.#field_name = *v;
                } else if let Some(v) = value.downcast_ref::<i32>() {
                    // Also try non-Option i32 for flexibility
                    println!("Setting i32 value to Optional<i32> field: field={}", stringify!(#field_name));
                    self.#field_name = Some(*v);
                }
            },
            "f64" => quote! {
                if let Some(v) = value.downcast_ref::<Option<f64>>() {
                    println!("Setting Optional f64 value to Individual: field={}, value={:?}", stringify!(#field_name), v);
                    self.#field_name = *v;
                } else if let Some(v) = value.downcast_ref::<f64>() {
                    // Also try non-Option f64 for flexibility
                    println!("Setting f64 value to Optional<f64> field: field={}", stringify!(#field_name));
                    self.#field_name = Some(*v);
                }
            },
            // For Vec types
            x if x.starts_with("Vec<") => {
                let vec_type = x.trim_start_matches("Vec<").trim_end_matches(">");
                
                match vec_type {
                    "String" => quote! {
                        if let Some(v) = value.downcast_ref::<Option<Vec<String>>>() {
                            self.#field_name = v.clone();
                        }
                    },
                    "NaiveDate" => quote! {
                        if let Some(v) = value.downcast_ref::<Option<Vec<chrono::NaiveDate>>>() {
                            self.#field_name = v.clone();
                        }
                    },
                    _ => quote! {
                        // Unknown vec type, just store in properties
                    },
                }
            },
            // Default case
            _ => quote! {
                // Unknown option type, just store in properties
            },
        }
    } else {
        // Non-Option types
        match field_type_to_string(field_type).as_str() {
            "String" => quote! {
                if let Some(v) = value.downcast_ref::<String>() {
                    println!("Setting string value to Individual: field={}, value={}", stringify!(#field_name), v);
                    self.#field_name = v.clone();
                } else if let Some(v) = value.downcast_ref::<Option<String>>() {
                    // Try to extract String from Option<String>
                    if let Some(inner_val) = v {
                        println!("Setting string from Option<String> to Individual: field={}, value={}", stringify!(#field_name), inner_val);
                        self.#field_name = inner_val.clone();
                    }
                }
            },
            "i32" => quote! {
                if let Some(v) = value.downcast_ref::<i32>() {
                    println!("Setting i32 value to Individual: field={}, value={}", stringify!(#field_name), v);
                    self.#field_name = *v;
                } else if let Some(v) = value.downcast_ref::<Option<i32>>() {
                    // Try to extract i32 from Option<i32>
                    if let Some(inner_val) = v {
                        println!("Setting i32 from Option<i32> to Individual: field={}, value={}", stringify!(#field_name), inner_val);
                        self.#field_name = *inner_val;
                    }
                }
            },
            "f64" => quote! {
                if let Some(v) = value.downcast_ref::<f64>() {
                    println!("Setting f64 value to Individual: field={}, value={}", stringify!(#field_name), v);
                    self.#field_name = *v;
                } else if let Some(v) = value.downcast_ref::<Option<f64>>() {
                    // Try to extract f64 from Option<f64>
                    if let Some(inner_val) = v {
                        println!("Setting f64 from Option<f64> to Individual: field={}, value={}", stringify!(#field_name), inner_val);
                        self.#field_name = *inner_val;
                    }
                }
            },
            "NaiveDate" => quote! {
                if let Some(v) = value.downcast_ref::<chrono::NaiveDate>() {
                    println!("Setting date value to Individual: field={}, value={}", stringify!(#field_name), v);
                    self.#field_name = *v;
                } else if let Some(v) = value.downcast_ref::<Option<chrono::NaiveDate>>() {
                    // Try to extract NaiveDate from Option<NaiveDate>
                    if let Some(inner_val) = v {
                        println!("Setting date from Option<NaiveDate> to Individual: field={}, value={}", stringify!(#field_name), inner_val);
                        self.#field_name = *inner_val;
                    }
                }
            },
            // Default case
            _ => quote! {
                // Unknown type, just store in properties
            },
        }
    }
}

/// Generate field getter code
#[allow(dead_code)]
fn generate_field_getter_code(field_name: &syn::Ident, _field_type: &Type) -> proc_macro2::TokenStream {
    quote! {
        Some(Box::new(self.#field_name.clone()))
    }
}

/// Extract the inner type of an Option<T>
fn extract_inner_option_type(ty: &Type) -> String {
    if let Type::Path(type_path) = ty {
        if let Some(segment) = type_path.path.segments.first() {
            if segment.ident == "Option" {
                if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                    if let Some(syn::GenericArgument::Type(inner_type)) = args.args.first() {
                        return field_type_to_string(inner_type);
                    }
                }
            }
        }
    }
    "Unknown".to_string()
}

/// Convert a field type to string
fn field_type_to_string(ty: &Type) -> String {
    if let Type::Path(type_path) = ty {
        if let Some(segment) = type_path.path.segments.first() {
            let type_name = segment.ident.to_string();
            
            // Check for generic types like Vec<String>
            if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                if let Some(syn::GenericArgument::Type(inner_type)) = args.args.first() {
                    if let Type::Path(inner_path) = inner_type {
                        if let Some(inner_segment) = inner_path.path.segments.first() {
                            return format!("{}<{}>", type_name, inner_segment.ident);
                        }
                    }
                }
            }
            
            return type_name;
        }
    }
    "Unknown".to_string()
}