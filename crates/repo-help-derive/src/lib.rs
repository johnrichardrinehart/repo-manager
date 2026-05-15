use proc_macro::TokenStream;
use quote::quote;
use syn::{
    Attribute, Data, DeriveInput, Error, Expr, ExprLit, Fields, Lit, Meta, Result, Token, Type,
    parse_macro_input, punctuated::Punctuated,
};

#[proc_macro_derive(HelpGroup, attributes(command, help_group))]
pub fn derive_help_group(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand_help_group(input)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

#[proc_macro_derive(HelpTemplate)]
pub fn derive_help_template(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand_help_template(input)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

fn expand_help_group(input: DeriveInput) -> Result<proc_macro2::TokenStream> {
    let ident = input.ident;
    let heading = group_heading(&input.attrs)?;
    let variants = enum_variants(&input.data)?;
    let mut command_exprs = Vec::new();

    for variant in variants {
        let name = command_name(&variant.attrs)?
            .unwrap_or_else(|| to_kebab_case(&variant.ident.to_string()));
        let about = command_about(&variant.attrs)?.ok_or_else(|| {
            Error::new_spanned(
                &variant.ident,
                "grouped help requires #[command(about = \"...\")] on every grouped subcommand",
            )
        })?;
        command_exprs.push(quote! {
            crate::HelpCommand {
                name: #name,
                about: #about,
            }
        });
    }

    Ok(quote! {
        impl crate::HelpGroup for #ident {
            fn help_group() -> crate::HelpCommandGroup {
                crate::HelpCommandGroup {
                    heading: #heading,
                    commands: vec![#(#command_exprs),*],
                }
            }
        }
    })
}

fn expand_help_template(input: DeriveInput) -> Result<proc_macro2::TokenStream> {
    let ident = input.ident;
    let variants = enum_variants(&input.data)?;
    let mut group_types = Vec::new();

    for variant in variants {
        let Fields::Unnamed(fields) = &variant.fields else {
            return Err(Error::new_spanned(
                &variant.ident,
                "grouped help root variants must wrap a subcommand-group enum",
            ));
        };
        if fields.unnamed.len() != 1 {
            return Err(Error::new_spanned(
                &variant.ident,
                "grouped help root variants must have exactly one field",
            ));
        }
        let Type::Path(group_type) = &fields.unnamed[0].ty else {
            return Err(Error::new_spanned(
                &fields.unnamed[0].ty,
                "grouped help root variants must use a named group type",
            ));
        };
        group_types.push(group_type);
    }

    Ok(quote! {
        impl crate::HelpTemplate for #ident {
            fn help_template() -> String {
                crate::render_help_template(vec![
                    #(<#group_types as crate::HelpGroup>::help_group()),*
                ])
            }
        }
    })
}

fn enum_variants(
    data: &Data,
) -> Result<&syn::punctuated::Punctuated<syn::Variant, syn::token::Comma>> {
    match data {
        Data::Enum(data) => Ok(&data.variants),
        _ => Err(Error::new(
            proc_macro2::Span::call_site(),
            "grouped help derives only support enums",
        )),
    }
}

fn group_heading(attrs: &[Attribute]) -> Result<String> {
    let Some(attr) = attrs.iter().find(|attr| attr.path().is_ident("help_group")) else {
        return Err(Error::new(
            proc_macro2::Span::call_site(),
            "missing #[help_group(title = \"...\")]",
        ));
    };

    let mut heading = None;
    attr.parse_nested_meta(|meta| {
        if meta.path.is_ident("title") {
            heading = Some(meta.value()?.parse::<Lit>()?);
            Ok(())
        } else {
            Err(meta.error("unsupported help_group attribute"))
        }
    })?;
    if let Some(Lit::Str(value)) = heading {
        Ok(value.value())
    } else {
        Err(Error::new_spanned(
            attr,
            "expected #[help_group(title = \"...\")]",
        ))
    }
}

fn command_name(attrs: &[Attribute]) -> Result<Option<String>> {
    command_lit_string(attrs, "name")
}

fn command_about(attrs: &[Attribute]) -> Result<Option<String>> {
    command_lit_string(attrs, "about")
}

fn command_lit_string(attrs: &[Attribute], key: &str) -> Result<Option<String>> {
    for attr in attrs.iter().filter(|attr| attr.path().is_ident("command")) {
        if let Meta::List(list) = &attr.meta {
            let entries = list.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;
            for entry in entries {
                let Meta::NameValue(name_value) = entry else {
                    continue;
                };
                if name_value.path.is_ident(key) {
                    return string_expr(&name_value.value).map(Some);
                }
            }
        }
    }

    Ok(None)
}

fn string_expr(expr: &Expr) -> Result<String> {
    match expr {
        Expr::Lit(ExprLit {
            lit: Lit::Str(value),
            ..
        }) => Ok(value.value()),
        _ => Err(Error::new_spanned(
            expr,
            "grouped help attributes must use string literals",
        )),
    }
}

fn to_kebab_case(input: &str) -> String {
    let mut output = String::new();
    for (index, ch) in input.char_indices() {
        if ch.is_uppercase() {
            if index > 0 {
                output.push('-');
            }
            output.extend(ch.to_lowercase());
        } else {
            output.push(ch);
        }
    }
    output
}
