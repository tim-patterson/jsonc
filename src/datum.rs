use std::collections::HashMap;

/// In memory row orientated layout for json like data
#[derive(Clone, Debug, PartialEq)]
pub enum Datum {
    Null,
    Missing,
    Float(f64),
    TinyInt(i8),
    SmallInt(i16),
    Bool(bool),
    String(String),
    Array(Vec<Datum>),
    Object(HashMap<String, Datum>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Type {
    Null,
    Missing,
    Float,
    TinyInt,
    SmallInt,
    Bool,
    String,
    Array,
    Object,
    Union,
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
            Datum::Object(_) => Type::Object,
            Datum::TinyInt(_) => Type::TinyInt,
            Datum::SmallInt(_) => Type::SmallInt,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Datum::Float(f) => Some(*f),
            Datum::SmallInt(i) => Some(*i as f64),
            Datum::TinyInt(i) => Some(*i as f64),
            _ => None,
        }
    }
}
