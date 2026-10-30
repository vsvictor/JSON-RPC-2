use proc_macro2::TokenStream;
use quote::quote;
use syn::{
    Data, DeriveInput, Error, Expr, Fields, GenericArgument, Lit, PathArguments, Result, Type,
    spanned::Spanned,
};

use crate::rpc_methods::string_value;

#[derive(Default)]
struct FieldOptions {
    key: Option<String>,
    default: Option<Expr>,
    min: Option<Expr>,
    with: Option<Expr>,
    nested: bool,
    port: bool,
    unit: Option<&'static str>,
}

pub fn expand(input: DeriveInput) -> Result<TokenStream> {
    let mut prefix = String::new();
    for attr in input.attrs.iter().filter(|a| a.path().is_ident("settings")) {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("prefix") {
                prefix = string_value(&meta)?;
                Ok(())
            } else {
                Err(meta.error("expected `prefix = \"...\"`"))
            }
        })?;
    }
    let Data::Struct(data) = &input.data else {
        return Err(Error::new(
            input.span(),
            "FromSettings can only be derived for structs",
        ));
    };
    let Fields::Named(fields) = &data.fields else {
        return Err(Error::new(
            input.span(),
            "FromSettings needs a struct with named fields",
        ));
    };
    let krate = quote!(::json_rpc_2_server);
    let mut values = Vec::new();
    for field in &fields.named {
        let ident = field.ident.as_ref().expect("named field");
        let options = field_options(field)?;
        let key = options
            .key
            .clone()
            .unwrap_or_else(|| format!("{prefix}{}", ident.to_string().to_uppercase()));
        let value = field_value(&field.ty, &key, &options, field.span())?;
        values.push(quote! { #ident: #value });
    }
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    Ok(quote! {
        impl #impl_generics #krate::FromSettings for #name #ty_generics #where_clause {
            fn from_settings(
                settings: &#krate::Settings<'_>,
            ) -> #krate::__private::anyhow::Result<Self> {
                ::std::result::Result::Ok(Self { #(#values,)* })
            }
        }
    })
}

fn field_options(field: &syn::Field) -> Result<FieldOptions> {
    let mut options = FieldOptions::default();
    for attr in field.attrs.iter().filter(|a| a.path().is_ident("setting")) {
        attr.parse_nested_meta(|meta| {
            let path = &meta.path;
            if path.is_ident("key") {
                options.key = Some(string_value(&meta)?);
            } else if path.is_ident("default") {
                options.default = Some(meta.value()?.parse()?);
            } else if path.is_ident("min") {
                options.min = Some(meta.value()?.parse()?);
            } else if path.is_ident("with") {
                options.with = Some(meta.value()?.parse()?);
            } else if path.is_ident("nested") {
                options.nested = true;
            } else if path.is_ident("port") {
                options.port = true;
            } else if path.is_ident("secs") {
                options.unit = Some("duration_secs");
            } else if path.is_ident("millis") {
                options.unit = Some("duration_millis");
            } else {
                return Err(meta.error(
                    "expected `key`, `default`, `min`, `with`, `nested`, `port`, `secs` or `millis`",
                ));
            }
            Ok(())
        })?;
    }
    Ok(options)
}

fn field_value(
    ty: &Type,
    key: &str,
    options: &FieldOptions,
    span: proc_macro2::Span,
) -> Result<TokenStream> {
    let krate = quote!(::json_rpc_2_server);
    if let Some(with) = &options.with {
        return Ok(quote! { (#with)(settings)? });
    }
    if options.nested {
        return Ok(quote! { <#ty as #krate::FromSettings>::from_settings(settings)? });
    }
    let default = options.default.as_ref();
    let value = if let Some(unit) = options.unit {
        let Some(default) = default else {
            return Err(Error::new(span, "`secs` / `millis` need a `default`"));
        };
        let method = syn::Ident::new(unit, span);
        quote! { settings.#method(#key, #default)? }
    } else if last_ident(ty).as_deref() == Some("Duration") {
        return Err(Error::new(
            span,
            "Duration fields need `secs` or `millis` and a `default`",
        ));
    } else if options.port {
        let Some(default) = default else {
            return Err(Error::new(span, "`port` needs a `default`"));
        };
        quote! { settings.port(#key, #default)? }
    } else if let Some(inner) = option_inner(ty) {
        if default.is_some() {
            return Err(Error::new(span, "Option fields cannot have a `default`"));
        }
        if last_ident(inner).as_deref() == Some("String") {
            quote! { settings.get(#key) }
        } else {
            quote! { settings.parse::<#inner>(#key)? }
        }
    } else if last_ident(ty).as_deref() == Some("String") {
        match default {
            Some(default) => quote! {
                settings.get(#key).unwrap_or_else(|| ::std::string::String::from(#default))
            },
            None => quote! { settings.required(#key)? },
        }
    } else {
        match default {
            Some(Expr::Lit(lit)) if matches!(lit.lit, Lit::Str(_)) => {
                quote! { settings.parse_or::<#ty>(#key, ::core::convert::Into::into(#lit))? }
            }
            Some(default) => quote! { settings.parse_or::<#ty>(#key, #default)? },
            None => quote! { settings.parse_required::<#ty>(#key)? },
        }
    };
    Ok(match &options.min {
        Some(min) => quote! { ::core::cmp::Ord::max(#value, #min) },
        None => value,
    })
}

fn last_ident(ty: &Type) -> Option<String> {
    match ty {
        Type::Path(path) => path
            .path
            .segments
            .last()
            .map(|segment| segment.ident.to_string()),
        _ => None,
    }
}

fn option_inner(ty: &Type) -> Option<&Type> {
    let Type::Path(path) = ty else { return None };
    let segment = path.path.segments.last()?;
    if segment.ident != "Option" {
        return None;
    }
    let PathArguments::AngleBracketed(args) = &segment.arguments else {
        return None;
    };
    match args.args.first()? {
        GenericArgument::Type(inner) => Some(inner),
        _ => None,
    }
}
