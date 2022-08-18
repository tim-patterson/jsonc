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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Type {
    Null,
    Missing,
    Float,
    Bool,
    String,
    Array,
    Object
}

impl Datum {
    pub fn is_null(&self) -> bool {
        matches!(self, Datum::Null)
    }

    pub fn is_missing(&self) -> bool {
        matches!(self, Datum::Missing)
    }

    pub fn type_of(&self) -> Type {
        match self {
            Datum::Null => Type::Null,
            Datum::Missing => Type::Missing,
            Datum::Float(_) => Type::Float,
            Datum::Bool(_) => Type::Bool,
            Datum::String(_) => Type::String,
            Datum::Array(_) => Type::Array,
            Datum::Object(_) => Type::Object
        }
    }
 }