//! Utility functions for procedural macros
//!
//! This module contains utility functions used by the procedural macros,
//! such as type checking, field mapping, and code generation.

use proc_macro2::TokenStream;
use quote::quote;
use syn::Type;

/// Check if a type is an Option<T>
pub fn is_option_type(ty: &Type) -> bool {
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
pub fn extract_field_type_info(ty: &Type) -> (TokenStream, TokenStream, TokenStream) {
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
pub fn extract_inner_type_info(ty: &Type) -> (TokenStream, TokenStream, TokenStream) {
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
pub fn get_rust_type(ty: &Type) -> TokenStream {
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

/// Generate setter code for a field
pub fn generate_field_setter_code(
    field_name: &syn::Ident,
    field_type: &Type,
    id_type: &str,
) -> TokenStream {
    let field_name_str = field_name.to_string();
    let is_option_field = is_option_type(field_type);

    // Special handling for different ID types
    if field_name_str == "pnr" && id_type == "pnr" {
        // This is the PNR field and we're using PNR as the ID
        return quote! {
            |individual, value| {
                // Cast to Individual
                let individual_obj = individual as &mut crate::models::core::Individual;
                individual_obj.pnr = value;
            }
        };
    } else if field_name_str == "record_number" && id_type == "record_number" {
        // This is the RECNUM field and we're using RECNUM as the ID
        if is_option_field {
            return quote! {
                |individual, value| {
                    // Cast to Individual
                    let individual_obj = individual as &mut crate::models::core::Individual;

                    // Store the record number in a property
                    individual_obj.set_property("record_number", Box::new(value));
                }
            };
        }
        return quote! {
            |individual, value| {
                // Cast to Individual
                let individual_obj = individual as &mut crate::models::core::Individual;

                // Store the record number in a property
                individual_obj.set_property("record_number", Box::new(value));
            }
        };
    } else if field_name_str == "dw_ek_kontakt" && id_type == "dw_ek_kontakt" {
        // This is the DW_EK_KONTAKT field and we're using it as the ID
        if is_option_field {
            return quote! {
                |individual, value| {
                    // Cast to Individual
                    let individual_obj = individual as &mut crate::models::core::Individual;

                    // Store the kontakt ID in a property
                    individual_obj.set_property("dw_ek_kontakt", Box::new(value));
                }
            };
        }
        return quote! {
            |individual, value| {
                // Cast to Individual
                let individual_obj = individual as &mut crate::models::core::Individual;

                // Store the kontakt ID in a property
                individual_obj.set_property("dw_ek_kontakt", Box::new(value));
            }
        };
    }

    // Special handling for date fields
    match field_name_str.as_str() {
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
                    let boxed_value: Box<dyn Any + Send + Sync> = Box::new(Some(value as NaiveDate));
                    individual_obj.set_property("event_date", boxed_value);
                }
            }
        }
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

                    // Cast to Individual
                    let individual_obj = individual as &mut crate::models::core::Individual;

                    // For Option<T> fields, we need to wrap the value in Some
                    // Box::new can't directly box None, so we need to create an Option first
                    let boxed_value: Box<dyn std::any::Any + Send + Sync> = Box::new(Some(value));
                    individual_obj.set_property(stringify!(#field_name), boxed_value);
                }
            }
        }
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

                    // Cast to Individual
                    let individual_obj = individual as &mut crate::models::core::Individual;

                    // Box the value directly for non-Option fields
                    let boxed_value: Box<dyn std::any::Any + Send + Sync> = Box::new(value);
                    individual_obj.set_property(stringify!(#field_name), boxed_value);
                }
            }
        }
    }
}
