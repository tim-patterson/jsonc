use std::collections::{BTreeMap};
use bit_vec::BitVec;
use crate::datum::{Datum, Type};

// ie for {a: 8}, {a: 9}, {}, {a: null}
// offset0 [1,2,2,3]
// data    [8,9,0]
// nulls   [f,f,t]
//
// for {a: [5]}, {a: []}, {a: [8,9]}
// offset0 [1,1,3]
// data    [5,8,9]
//
// Explicit nulls still have a slot in the vector but shouldn't be read.
// {foo: {bar: 5}}
// {foo: {bar: null}}
// {foo: null}
// foo -> object{indexes=[[1,2,3]], nulls=[f,f,t], size=[1,1,0]}
// foo.bar -> number{offsets=[[1,2]], nulls=[f,t], vals=[5,0]}
//
// {foo: [{bar: 5}]}
// foo -> array{offsets=[[1]], nulls=[f], size=[1]}
// foo.[] -> object{offsets=[[1],[1]], nulls=[f], size=[1]}
// foo.[].bar -> number{offsets=[[1],[1]], nulls=[f], vals=[5]}
//
// {foo: [[{bar: 5}]]
// foo -> array{offsets=[[1]], nulls=[f], size=[1]}
// foo.[] -> array{offsets=[[1],[1]], nulls=[f], size=[1]}
// foo.[].[] -> object{offsets=[[1],[1],[1]], nulls=[f], size=[1]}
// foo.[].[].bar -> number{offsets=[[1],[1],[1]],nulls=[f], vals=[5]}
//
// {foo: {bar: 5}}
// {foo: [{bar: 5}]}
// foo -> union{offsets=[[1,2]], nulls=[f,f] vals=[object{size=1}, array{size=1}]}
// foo.bar -> number{offsets=[[1,1]], nulls=[f], vals=[5]}
// foo.[] -> object{offsets=[[0,1]], nulls=[t], size=[1]}
// foo.[].bar -> number{offsets=[[0,1]], nulls=[f], vals=[5]}


// Ideas:
// * Does it make sense to store arrow like offsets or are we better just storing indexes?
//   point lookups would require binary searches but scans is what we want to be good at anyway.
//   If we did this then for sparse data the top level offsets would be much smaller, but we'd pay
//   the cost of duplicating parent indexes for arrays(could be rle compressed?)
//   Or maybe our data structure could instead only track where these change aka dremel.
//
// * have offsets at root but swap out offset array with a smarter data structure,
//   ask for offsets in blocks of 16, ie 64-79 and it either returns a constant inc mapping
//   (0 - no data, 1 - ie required top level field, 2 - an array that every item has 2 instances of).
//   advantages: no bloat for required fields nor rare fields. special paths in code could take
//   advantage of common cases



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
#[derive(Debug)]
pub struct Stripe {
    columns: BTreeMap<Path, Column>,
    count: usize
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
        self.push_datum_at_path(datum, &[], &[self.count]);
        self.count += 1;
    }

    pub fn get_column(&self, path: &[PathComponent]) -> Option<&Column> {
        self.columns.get(path)
    }

    fn push_datum_at_path(&mut self, datum: &Datum, path: &[PathComponent], indexes: &[usize]) {
        if datum.is_missing() {
            return;
        }
        if !self.columns.contains_key(path) {
            self.columns.insert(path.to_vec(), Column::new(indexes.len()));
        }
        let column = self.columns.get_mut(path).unwrap();
        column.add_datum(datum, indexes);

        match datum {
            Datum::Object(obj) => {
                for (key, value) in obj.iter() {
                    let mut child_path = path.to_vec();
                    child_path.push(PathComponent::Key(key.clone()));
                    self.push_datum_at_path(value, &child_path, indexes);
                }
            }
            Datum::Array(arr) => {
                let mut child_path = path.to_vec();
                child_path.push(PathComponent::Array);
                let mut child_indexes = indexes.to_vec();
                child_indexes.push(0);

                for (idx, datum) in arr.iter().enumerate() {
                    *child_indexes.last_mut().unwrap() = idx;
                    // Should we push down indexes here or repeat level?,
                    // for columns that start part way through the data stream, we'll need to pad
                    // out the array, at least at the top level...
                    self.push_datum_at_path(datum, &child_path, &child_indexes);
                }
            }
            _ => {}
        }
    }
}

/// Represents the data at a given path
#[derive(Debug)]
pub struct Column {
    indexes: Vec<Vec<u16>>,
    pub data: ColumnData,
    pub null_map: BitVec
}

impl Column {
    fn new(depth: usize) -> Self {
        Column {
            indexes: vec![Vec::new(); depth],
            data: ColumnData::Null,
            null_map: BitVec::new()
        }
    }

    fn add_datum(&mut self, datum: &Datum, indexes: &[usize]) {
        self.up_cast(datum.type_of());
        for (index, index_buf) in indexes.iter().zip(self.indexes.iter_mut()) {
            index_buf.push(*index as u16);
        }
        self.null_map.push(datum.is_null());
        match (&mut self.data, datum) {
            (ColumnData::Null, Datum::Null) => {},
            (ColumnData::Null, _) => unreachable!(),
            (ColumnData::Bool(vec), Datum::Bool(b)) => vec.push(*b),
            (ColumnData::Bool(vec), Datum::Null) => vec.push(false),
            (ColumnData::Bool(_), _) => unreachable!(),
            (ColumnData::Float(vec), Datum::Float(f)) => vec.push(*f),
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
            (_, Type::Missing) |
            (_, Type::Null) |
            (ColumnData::Union(_), _) => {}
            // Column type matches, we're ok
            (ColumnData::Float(_), Type::Float) |
            (ColumnData::Array(_), Type::Array) |
            (ColumnData::String(_, _), Type::String) |
            (ColumnData::Object(_), Type::Object) |
            (ColumnData::Bool(_), Type::Bool) => {}
            // Column type is null, just upcast, padding with default values
            (ColumnData::Null, Type::Bool) => {
                let mut vec = BitVec::new();
                vec.grow(self.null_map.len(), false);
                self.data = ColumnData::Bool(vec);
            }
            (ColumnData::Null, Type::Float) => {
                self.data = ColumnData::Float(vec![0.0;self.null_map.len()]);
            }
            (ColumnData::Null, Type::Object) => {
                self.data = ColumnData::Object(vec![0;self.null_map.len()]);
            }
            (ColumnData::Null, Type::Array) => {
                self.data = ColumnData::Array(vec![0;self.null_map.len()]);
            }
            (ColumnData::Null, Type::String) => {
                self.data = ColumnData::String(String::new(),vec![0;self.null_map.len()]);
            }
            // Otherwise we have to convert to a union type
            (_, _) => {
                todo!()
            }
        }
    }
}

/// The actual data inside one column
#[derive(Debug)]
pub enum ColumnData {
    Null, // If the whole column is null and untyped.
    Float(Vec<f64>),
    Bool(BitVec),
    String(String, Vec<usize>),
    Object(Vec<usize>),
    Array(Vec<usize>),
    Union(Vec<Union>)
}

impl ColumnData {
    pub fn is_null(&self) -> bool {
        matches!(self, ColumnData::Null)
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
    Object(usize)
}