use std::collections::HashMap;

/// In memory row orientated layout for json like data
#[derive(Clone, Debug)]
pub enum Datum {
    Null,
    Float(f64),
    Bool(bool),
    String(String),
    Array(Vec<Datum>),
    Object(HashMap<String, Datum>)
}