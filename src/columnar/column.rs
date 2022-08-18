use crate::datum::{Datum, Type};
use bit_vec::BitVec;

/// Represents the data at a given path
#[derive(Debug)]
pub struct Column {
    indexes: Vec<Vec<u16>>,
    pub data: ColumnData,
    pub null_map: BitVec,
}

impl Column {
    pub(super) fn new(depth: usize) -> Self {
        Column {
            indexes: vec![Vec::new(); depth],
            data: ColumnData::Null,
            null_map: BitVec::new(),
        }
    }

    pub(super) fn add_datum(&mut self, datum: &Datum, indexes: &[usize]) {
        self.up_cast(datum.type_of());
        for (index, index_buf) in indexes.iter().zip(self.indexes.iter_mut()) {
            index_buf.push(*index as u16);
        }
        self.null_map.push(datum.is_null());
        match (&mut self.data, datum) {
            (ColumnData::Null, Datum::Null) => {}
            (ColumnData::Null, _) => unreachable!(),
            (ColumnData::Bool(vec), Datum::Bool(b)) => vec.push(*b),
            (ColumnData::Bool(vec), Datum::Null) => vec.push(false),
            (ColumnData::Bool(_), _) => unreachable!(),
            (ColumnData::TinyInt(vec), Datum::TinyInt(i)) => vec.push(*i),
            (ColumnData::TinyInt(vec), Datum::Null) => vec.push(0),
            (ColumnData::TinyInt(_), _) => unreachable!(),
            (ColumnData::SmallInt(vec), Datum::SmallInt(i)) => vec.push(*i),
            (ColumnData::SmallInt(vec), Datum::TinyInt(i)) => vec.push(*i as i16),
            (ColumnData::SmallInt(vec), Datum::Null) => vec.push(0),
            (ColumnData::SmallInt(_), _) => unreachable!(),
            (ColumnData::Float(vec), Datum::Float(f)) => vec.push(*f),
            (ColumnData::Float(vec), Datum::SmallInt(i)) => vec.push(*i as f64),
            (ColumnData::Float(vec), Datum::TinyInt(i)) => vec.push(*i as f64),
            (ColumnData::Float(vec), Datum::Null) => vec.push(0.0),
            (ColumnData::Float(_), _) => unreachable!(),
            (ColumnData::String(str_buf, offsets), Datum::String(str)) => {
                str_buf.push_str(str);
                offsets.push(str_buf.len());
            }
            (ColumnData::String(str_buf, offsets), Datum::Null) => {
                offsets.push(str_buf.len());
            }
            (ColumnData::String(_, _), _) => unreachable!(),
            (ColumnData::Array(sizes), Datum::Array(arr)) => sizes.push(arr.len()),
            (ColumnData::Array(sizes), Datum::Null) => sizes.push(0),
            (ColumnData::Array(_), _) => unreachable!(),
            (ColumnData::Object(sizes), Datum::Object(obj)) => sizes.push(obj.len()),
            (ColumnData::Object(sizes), Datum::Null) => sizes.push(0),
            (ColumnData::Object(_), _) => unreachable!(),
            (ColumnData::Union(vec), Datum::Null) => vec.push(Union::Null),
            (ColumnData::Union(_), Datum::Missing) => unreachable!(),
            (ColumnData::Union(vec), Datum::Bool(b)) => vec.push(Union::Bool(*b)),
            (ColumnData::Union(vec), Datum::TinyInt(i)) => vec.push(Union::Float(*i as f64)),
            (ColumnData::Union(vec), Datum::SmallInt(i)) => vec.push(Union::Float(*i as f64)),
            (ColumnData::Union(vec), Datum::Float(f)) => vec.push(Union::Float(*f)),
            (ColumnData::Union(vec), Datum::String(s)) => vec.push(Union::String(s.clone())),
            (ColumnData::Union(vec), Datum::Object(obj)) => vec.push(Union::Object(obj.len())),
            (ColumnData::Union(vec), Datum::Array(arr)) => vec.push(Union::Array(arr.len())),
        }
    }

    /// Up-casts the columnData to be of the type needed to accept the passed in datum
    fn up_cast(&mut self, data_type: Type) {
        match (&self.data, data_type) {
            // Null data or union columns are like wildcards.
            (_, Type::Missing) | (_, Type::Null) | (ColumnData::Union(_), _) => {}
            // Column type matches, we're ok
            (ColumnData::Float(_), Type::Float)
            | (ColumnData::TinyInt(_), Type::TinyInt)
            | (ColumnData::SmallInt(_), Type::SmallInt)
            | (ColumnData::Array(_), Type::Array)
            | (ColumnData::String(_, _), Type::String)
            | (ColumnData::Object(_), Type::Object)
            | (ColumnData::Bool(_), Type::Bool) => {}
            // Compatible columns
            (ColumnData::SmallInt(_), Type::TinyInt)
            | (ColumnData::Float(_), Type::TinyInt)
            | (ColumnData::Float(_), Type::SmallInt) => {}
            // Column type is null, just upcast, padding with default values
            (ColumnData::Null, Type::Bool) => {
                let mut vec = BitVec::new();
                vec.grow(self.null_map.len(), false);
                self.data = ColumnData::Bool(vec);
            }
            (ColumnData::Null, Type::TinyInt) => {
                self.data = ColumnData::TinyInt(vec![0; self.null_map.len()]);
            }
            (ColumnData::Null, Type::SmallInt) => {
                self.data = ColumnData::SmallInt(vec![0; self.null_map.len()]);
            }
            (ColumnData::Null, Type::Float) => {
                self.data = ColumnData::Float(vec![0.0; self.null_map.len()]);
            }
            (ColumnData::Null, Type::Object) => {
                self.data = ColumnData::Object(vec![0; self.null_map.len()]);
            }
            (ColumnData::Null, Type::Array) => {
                self.data = ColumnData::Array(vec![0; self.null_map.len()]);
            }
            (ColumnData::Null, Type::String) => {
                self.data = ColumnData::String(String::new(), vec![0; self.null_map.len()]);
            }
            // Special cases to upcast numeric types
            (ColumnData::TinyInt(vec), Type::SmallInt) => {
                self.data = ColumnData::SmallInt(vec.iter().map(|i| *i as i16).collect())
            }
            (ColumnData::TinyInt(vec), Type::Float) => {
                self.data = ColumnData::Float(vec.iter().map(|i| *i as f64).collect())
            }
            (ColumnData::SmallInt(vec), Type::Float) => {
                self.data = ColumnData::Float(vec.iter().map(|i| *i as f64).collect())
            }

            // Otherwise we have to convert to a union type
            (col, datum) => {
                todo!("Tried to upcast {:?} to fit {:?}", col.type_for(), datum);
            }
        }
    }
}

/// The actual data inside one column
#[derive(Debug)]
pub enum ColumnData {
    Null, // If the whole column is null and untyped.
    TinyInt(Vec<i8>),
    SmallInt(Vec<i16>),
    Float(Vec<f64>),
    Bool(BitVec),
    String(String, Vec<usize>),
    Object(Vec<usize>),
    Array(Vec<usize>),
    Union(Vec<Union>),
}

impl ColumnData {
    pub fn is_null(&self) -> bool {
        matches!(self, ColumnData::Null)
    }

    pub fn type_for(&self) -> Type {
        match self {
            ColumnData::Null => Type::Null,
            ColumnData::TinyInt(_) => Type::TinyInt,
            ColumnData::SmallInt(_) => Type::SmallInt,
            ColumnData::Float(_) => Type::Float,
            ColumnData::Bool(_) => Type::Bool,
            ColumnData::String(_, _) => Type::String,
            ColumnData::Object(_) => Type::Object,
            ColumnData::Array(_) => Type::Array,
            ColumnData::Union(_) => Type::Union,
        }
    }
}

/// Very similar to a datum but Arrays and Objects only contain some metadata here.
#[derive(Clone, Debug, PartialEq)]
pub enum Union {
    Null,
    Float(f64),
    Bool(bool),
    String(String),
    Array(usize),
    Object(usize),
}
