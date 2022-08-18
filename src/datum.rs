use std::collections::HashMap;

/// "row orientated" layout for json-like data, used as an intermediate while loading data etc.
#[derive(Clone, Debug, PartialEq)]
pub enum Datum {
    Null,
    /// Represents a missing value, ie the difference between
    /// {foo: null} and {} when looking up foo
    Missing,
    Float(f64),
    TinyInt(i8),
    SmallInt(i16),
    Bool(bool),
    String(String),
    Array(Vec<Datum>),
    Object(HashMap<String, Datum>),
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum JsonType {
    Null,
    Missing,
    Number,
    Bool,
    String,
    Array,
    Object
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum InternalType {
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
    pub fn json_type(&self) -> JsonType {
        match self {
            Datum::Null => JsonType::Null,
            Datum::Missing => JsonType::Missing,
            Datum::Bool(_) => JsonType::Bool,
            Datum::Float(_) |
            Datum::TinyInt(_) |
            Datum::SmallInt(_) => JsonType::Number,
            Datum::String(_) => JsonType::String,
            Datum::Array(_) => JsonType::Array,
            Datum::Object(_) => JsonType::Object
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Datum::Null)
    }

    pub fn is_missing(&self) -> bool {
        matches!(self, Datum::Missing)
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Datum::Float(f) => Some(*f),
            Datum::SmallInt(i) => Some(*i as f64),
            Datum::TinyInt(i) => Some(*i as f64),
            _ => None,
        }
    }

    pub(crate) fn internal_type(&self) -> InternalType {
        match self {
            Datum::Null => InternalType::Null,
            Datum::Missing => InternalType::Missing,
            Datum::Float(_) => InternalType::Float,
            Datum::Bool(_) => InternalType::Bool,
            Datum::String(_) => InternalType::String,
            Datum::Array(_) => InternalType::Array,
            Datum::Object(_) => InternalType::Object,
            Datum::TinyInt(_) => InternalType::TinyInt,
            Datum::SmallInt(_) => InternalType::SmallInt,
        }
    }
}
