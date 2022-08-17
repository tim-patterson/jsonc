use std::collections::{BTreeMap};
use bit_vec::BitVec;
use crate::datum::Datum;

// {foo: {bar: 5}}
// foo -> object_column (size and null map) (1 offsets array)
// foo.bar -> number_column (1 offsets array)
//
// {foo: [{bar: 5}]}
// foo -> array_column (size and null map)
// foo.[] -> object_column (size and null map)
// foo.[].bar -> number_column
//
// {foo: [[{bar: 5}]]
// foo -> array_column (size and null map)
// foo.[] -> array_column (size and null map)
// foo.[].[] -> object_column (size and null map)
// foo.[].[].bar -> number_column
//
// {foo: {bar: 5}}
// {foo: [{bar: 5}]}
// foo -> union_column(array | object)
// foo.bar -> number_column
// foo.[] -> object_column (size and null map)
// foo.[].bar -> number_column

/// A path to a json node
pub type Path = Vec<PathComponent>;

/// A segment of a path to a json node.
/// Array offsets aren't stored with the
#[derive(Debug, Eq, PartialEq, Clone, Ord, PartialOrd)]
pub enum PathComponent {
    Key(String),
    Array
}

/// A chunk of data that's been serialized in one go.
/// Indexes within the data are all stripe local
pub struct Stripe {
    columns: BTreeMap<Path, Column>,
    count: u16
}

impl Stripe {
    pub fn new() -> Self {
        Stripe {
            columns: BTreeMap::new(),
            count: 0
        }
    }

    /// Push a datum into the stripe
    pub fn push_datum(&mut self, datum: &Datum) {
        self.push_datum_at_path(datum, &[], &[])
    }

    pub fn get_column(&self, path: &[PathComponent]) -> Option<&Column> {
        self.columns.get(path)
    }

    fn push_datum_at_path(&mut self, datum: &Datum, path: &[PathComponent], offsets: &[usize]) {
        if datum.is_missing() {
            return;
        }
        if !self.columns.contains_key(path) {
            self.columns.insert(path.to_vec(), Column::new());
        }
        let column = self.columns.get_mut(path).unwrap();
        column.add_datum(datum, offsets);

        match datum {
            Datum::Object(obj) => {
                for (key, value) in obj.iter() {
                    let mut child_path = path.to_vec();
                    child_path.push(PathComponent::Key(key.clone()));
                    self.push_datum_at_path(value, &child_path, offsets);
                }
            }
            Datum::Array(arr) => {
                let mut child_path = path.to_vec();
                child_path.push(PathComponent::Array);
                let mut child_offsets = offsets.to_vec();
                child_offsets.push(0);

                for (idx, datum) in arr.iter().enumerate() {
                    *child_offsets.last_mut().unwrap() = idx;
                    self.push_datum_at_path(datum, &child_path, &child_offsets);
                }
            }
            _ => {}
        }
    }
}

/// Represents the data at a given path
pub struct Column {
    // one index buffer for each layer of nesting, earlier indexes are pointers into the later
    // indexes, the last index being a pointer into the actual column data
    offsets: Vec<Vec<u32>>,
    pub data: ColumnData,
    // explicit nulls
    null_map: BitVec,
}

impl Column {
    fn new() -> Self {
        Column {
            offsets: Vec::new(),
            data: ColumnData::Null,
            null_map: BitVec::new()
        }
    }

    fn add_datum(&mut self, datum: &Datum, offsets: &[usize]) {
        self.null_map.push(datum.is_null());
        match datum {
            Datum::Null => {},
            Datum::Missing => unreachable!(),
            Datum::Float(f) => {
                if self.data.is_null() {
                    self.data = ColumnData::Float(Vec::new());
                }
                if let ColumnData::Float(vec) = &mut self.data {
                    vec.push(*f);
                } else {
                    panic!()
                }
            }
            Datum::Bool(b) => {
                if self.data.is_null() {
                    self.data = ColumnData::Bool(BitVec::new());
                }
                if let ColumnData::Bool(vec) = &mut self.data {
                    vec.push(*b);
                } else {
                    panic!()
                }
            }
            Datum::String(s) => {
                if self.data.is_null() {
                    self.data = ColumnData::String(String::new(), Vec::new());
                }
                if let ColumnData::String(string_buf, str_offsets) = &mut self.data {
                    string_buf.push_str(s);
                    str_offsets.push(string_buf.len());
                } else {
                    panic!()
                }
            }
            Datum::Array(a) => {
                if self.data.is_null() {
                    self.data = ColumnData::Array(Vec::new());
                }
                if let ColumnData::Array(sizes) = &mut self.data {
                    sizes.push(a.len());
                } else {
                    panic!()
                }
            }
            Datum::Object(o) => {
                if self.data.is_null() {
                    self.data = ColumnData::Object(Vec::new());
                }
                if let ColumnData::Object(sizes) = &mut self.data {
                    sizes.push(o.len());
                } else {
                    panic!()
                }
            }
        }
    }
}

/// The actual data inside one column
pub enum ColumnData {
    Null, // If the whole column is null and untyped.
    Float(Vec<f64>),
    Bool(BitVec),
    String(String, Vec<usize>),
    Object(Vec<usize>),
    Array(Vec<usize>),
    Union(Union)
}

impl ColumnData {
    pub fn is_null(&self) -> bool {
        if let ColumnData::Null = self {
            true
        } else {
            false
        }
    }
}

/// Very similar to a datum but Arrays and Objects only contain some metadata here.
#[derive(Clone, Debug, PartialEq)]
pub enum Union {
    Float(f64),
    Bool(bool),
    String(String),
    Array(usize),
    Object(usize)
}