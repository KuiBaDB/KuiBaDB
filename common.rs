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
use std::io::Read;
use yaml_rust::{Yaml, YamlLoader};

pub fn load_yaml(filepath: &str) -> anyhow::Result<Vec<Yaml>> {
    let mut cfgfile = std::fs::File::open(filepath)?;
    let mut confstr = String::new();
    cfgfile.read_to_string(&mut confstr)?;
    let conf = YamlLoader::load_from_str(&confstr)?;
    Ok(conf)
}

pub fn yaml_try_tostr(input: &Yaml) -> Option<String> {
    match input {
        Yaml::Real(v) => Some(v.to_string()),
        Yaml::Integer(v) => Some(v.to_string()),
        Yaml::String(v) => Some(v.to_string()),
        Yaml::Boolean(v) => Some(v.to_string()),
        _ => None,
    }
}
