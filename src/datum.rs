use std::collections::HashMap;

/// In memory row orientated layout for json like data
#[derive(Clone, Debug, PartialEq)]
pub enum Datum {
    Null,
    Missing,
    Float(f64),
    Bool(bool),
    String(String),
    Array(Vec<Datum>),
    Object(HashMap<String, Datum>)
}

impl Datum {
    pub fn is_null(&self) -> bool {
        if let Datum::Null = self {
            true
        } else {
            false
        }
    }

    pub fn is_missing(&self) -> bool {
        if let Datum::Missing = self {
            true
        } else {
            false
        }
    }
}