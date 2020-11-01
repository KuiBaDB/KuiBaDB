/*
Copyright 2020 <盏一 w@hidva.com>
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
#![allow(dead_code)]
use proc_macro2::{Literal, TokenStream};
use quote::{format_ident, quote, ToTokens};
use std::env;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use yaml_rust::Yaml;

mod common;

#[derive(Default)]
struct Guc {
    name: String,
    context: String,
    short_desc: String,
    vartype: String,
    boot_val: String,
    flags: String, // zero or flags.
    long_desc: Option<String>,
    preassign: Option<String>,
    show: Option<String>,
}

#[derive(Default)]
struct Gucs {
    bool_gucs: Vec<Guc>,
    int_gucs: Vec<Guc>,
    str_gucs: Vec<Guc>,
    real_gucs: Vec<Guc>,
}

impl Guc {
    fn new(val: &Yaml) -> Guc {
        Guc {
            name: val["name"].as_str().unwrap().to_string(),
            context: val["context"].as_str().unwrap().to_string(),
            short_desc: val["short_desc"].as_str().unwrap().to_string(),
            vartype: val["vartype"].as_str().unwrap().to_string(),
            boot_val: common::yaml_tostr(&val["boot_val"]),
            flags: val["flags"].as_str().or(Some("0")).unwrap().to_string(),
            long_desc: val["long_desc"].as_str().map(|v| v.to_string()),
            preassign: val["preassign"].as_str().map(|v| v.to_string()),
            show: val["show"].as_str().map(|v| v.to_string()),
        }
    }
}

fn load_gucs() -> Gucs {
    let mut gucs = Gucs::default();
    const INPUTFILE: &str = "./src/bin/kuiba/guc/gucdef.yaml";
    let yamldocs = common::load_yaml(INPUTFILE).unwrap();
    let yamldoc = &yamldocs[0];
    for gucdat in yamldoc.as_vec().unwrap() {
        let vartype = gucdat["vartype"].as_str().unwrap();
        let guc = Guc::new(gucdat);
        match vartype {
            "INT" => gucs.int_gucs.push(guc),
            "BOOL" => gucs.bool_gucs.push(guc),
            "STR" => gucs.str_gucs.push(guc),
            "REAL" => gucs.real_gucs.push(guc),
            _ => panic!(format!("Unknown vartype. vartype={}", vartype)),
        }
    }
    gucs
}

fn option_tokenstream(input: &Option<String>, asstr: bool) -> TokenStream {
    match input {
        None => "None".parse().unwrap(),
        Some(v) => {
            let val = if asstr {
                Literal::string(v).into_token_stream()
            } else {
                v.parse().unwrap()
            };
            quote! { Some(#val) }
        }
    }
}

struct FormatRet {
    name: Vec<TokenStream>,
    enumitem: Vec<TokenStream>,
    boot_val: Vec<TokenStream>,
}

// var_logical_type is the logical type of guc, such as Str, Int,
fn format_gucs(f: &mut File, var_logical_type: &'static str, gucs: &Vec<Guc>) -> FormatRet {
    let mut name = Vec::<TokenStream>::new();
    let mut context = Vec::<TokenStream>::new();
    let mut short_desc = Vec::<TokenStream>::new();
    let mut vartype = Vec::<TokenStream>::new(); // var enum type, See guc::Type.
    let mut boot_val = Vec::<TokenStream>::new();
    let mut long_desc = Vec::<TokenStream>::new();
    let mut flags = Vec::<TokenStream>::new();
    let mut preassign = Vec::<TokenStream>::new();
    let mut show = Vec::<TokenStream>::new();
    let mut enumitem = Vec::<TokenStream>::new();
    for guc in gucs {
        name.push(Literal::string(&guc.name).into_token_stream());
        context.push(guc.context.parse().unwrap());
        short_desc.push(Literal::string(&guc.short_desc).into_token_stream());
        vartype.push(guc.vartype.parse().unwrap());
        boot_val.push(if var_logical_type != "Str" {
            guc.boot_val.parse().unwrap()
        } else {
            Literal::string(&guc.boot_val).into_token_stream()
        });
        flags.push(guc.flags.parse().unwrap());
        long_desc.push(option_tokenstream(&guc.long_desc, true));
        preassign.push(option_tokenstream(&guc.preassign, false));
        show.push(option_tokenstream(&guc.show, false));
        enumitem.push(guc.name.to_ascii_uppercase().parse().unwrap());
    }

    let const_type = format_ident!("{}", var_logical_type);
    let const_size = gucs.len();
    let const_ident = format_ident!("{}_GUCS", var_logical_type.to_ascii_uppercase());
    let enum_ident = format_ident!("{}", var_logical_type.chars().nth(0).unwrap());
    let guctts = quote! {
        #(
            #const_type {
                gen: Generic {
                    name: #name,
                    context: #context,
                    short_desc: #short_desc,
                    long_desc: #long_desc,
                    flags: #flags,
                    show: #show,
                    vartype: #vartype,
                },
                boot_val: #boot_val,
                preassign: #preassign,
            }
        ),*
    };

    write!(
        f,
        "{}",
        quote! {
            pub const #const_ident: [#const_type; #const_size] = [
                #guctts
            ];
        }
    )
    .unwrap();
    write!(f, "\n").unwrap();
    write!(
        f,
        "{}",
        quote! {
            #[allow(non_camel_case_types)]
            #[derive(Copy, Clone)]
            pub enum #enum_ident {
                #(#enumitem,)*
                TOTAL_NUM,
            }
        }
    )
    .unwrap();
    FormatRet {
        name,
        enumitem,
        boot_val,
    }
}

fn gen_gucdef() {
    let gucs = load_gucs();
    let out_dir = env::var_os("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join("gucdef.rs");
    let mut outputf = File::create(dest_path).unwrap();
    write!(
        &mut outputf,
        "
    /*
    Copyright 2020 <盏一 w@hidva.com>
    Licensed under the Apache License, Version 2.0 (the \"License\");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an \"AS IS\" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
    */

    "
    )
    .unwrap();

    let bool_formatret = format_gucs(&mut outputf, "Bool", &gucs.bool_gucs);
    let bool_boot_vals = &bool_formatret.boot_val;
    write!(&mut outputf, "\n").unwrap();
    let int_formatret = format_gucs(&mut outputf, "Int", &gucs.int_gucs);
    let int_boot_vals = &int_formatret.boot_val;
    write!(&mut outputf, "\n").unwrap();
    let str_formatret = format_gucs(&mut outputf, "Str", &gucs.str_gucs);
    let str_boot_vals = &str_formatret.boot_val;
    write!(&mut outputf, "\n").unwrap();
    let real_formatret = format_gucs(&mut outputf, "Real", &gucs.real_gucs);
    let real_boot_vals = &real_formatret.boot_val;
    write!(&mut outputf, "\n").unwrap();

    let bool_guc_num = gucs.bool_gucs.len();
    let str_guc_num = gucs.str_gucs.len();
    let int_guc_num = gucs.int_gucs.len();
    let real_guc_num = gucs.real_gucs.len();
    // the type of str_boot_vals is &'static str.
    // and the type of string_boot_vals is String
    let mut string_boot_vals = Vec::<TokenStream>::new();
    for strval in str_boot_vals {
        string_boot_vals.push(quote! {
            #strval.to_string()
        })
    }
    write!(
        &mut outputf,
        "{}",
        quote! {
            #[derive(Clone)]
            pub struct GucVals {
                pub bool_vals: [bool; #bool_guc_num],
                pub str_vals: [String; #str_guc_num],
                pub int_vals: [i32; #int_guc_num],
                pub real_vals: [f64; #real_guc_num],
            }

            impl std::default::Default for GucVals {
                fn default() -> Self {
                    GucVals {
                        bool_vals: [
                            #( #bool_boot_vals ),*
                        ],
                        str_vals: [
                            #( #string_boot_vals ),*
                        ],
                        int_vals: [
                            #( #int_boot_vals ),*
                        ],
                        real_vals: [
                            #( #real_boot_vals ),*
                        ]
                    }
                }
            }
        }
    )
    .unwrap();

    write!(&mut outputf, "\n").unwrap();

    let bool_enumitem = &bool_formatret.enumitem;
    let bool_name = &bool_formatret.name;
    let int_enumitem = &int_formatret.enumitem;
    let int_name = &int_formatret.name;
    let real_enumitem = &real_formatret.enumitem;
    let real_name = &real_formatret.name;
    let str_enumitem = &str_formatret.enumitem;
    let str_name = &str_formatret.name;
    write!(
        &mut outputf,
        "{}",
        quote! {
            #[derive(Copy , Clone)]
            pub enum GucIdx {
                I(I),
                B(B),
                S(S),
                R(R)
            }

            lazy_static::lazy_static! {
                pub static ref GUC_NAMEINFO_MAP: HashMap<&'static str, GucIdx> = {
                    let mut m = HashMap::new();
                    #(m.insert(#bool_name, GucIdx::B(B::#bool_enumitem));)*
                    #(m.insert(#int_name, GucIdx::I(I::#int_enumitem));)*
                    #(m.insert(#str_name, GucIdx::S(S::#str_enumitem));)*
                    #(m.insert(#real_name, GucIdx::R(R::#real_enumitem));)*
                    m
                };
            }
        }
    )
    .unwrap();
}

fn copy_common() {
    const INPUT: &str = "./common.rs";
    let out_dir = env::var_os("OUT_DIR").unwrap();
    let output = Path::new(&out_dir).join("common.rs");
    std::fs::copy(INPUT, output).unwrap();
}

fn main() {
    gen_gucdef();
    copy_common();
}
