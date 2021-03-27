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
include!(concat!(env!("OUT_DIR"), "/common.rs"));

pub fn yaml_try_tobool(input: &Yaml) -> Option<bool> {
    match input {
        &Yaml::Integer(v) => Some(v != 0),
        Yaml::String(v) => Some(v.eq_ignore_ascii_case("true")),
        &Yaml::Boolean(v) => Some(v),
        _ => None,
    }
}

pub fn yaml_try_toi32(input: &Yaml) -> Option<i32> {
    match input {
        &Yaml::Integer(v) => Some(v as i32),
        Yaml::String(v) => v.parse().ok(),
        &Yaml::Boolean(v) => Some(if v { 1 } else { 0 }),
        Yaml::Real(v) => v.parse().ok(),
        _ => None,
    }
}

pub fn yaml_try_tof64(input: &Yaml) -> Option<f64> {
    match input {
        &Yaml::Integer(v) => Some(v as f64),
        Yaml::String(v) => v.parse().ok(),
        Yaml::Real(v) => v.parse().ok(),
        _ => None,
    }
}
