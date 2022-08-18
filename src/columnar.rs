mod column;
use crate::columnar::column::Column;
pub use crate::columnar::column::ColumnData;
use crate::datum::Datum;
use std::collections::BTreeMap;
use serde::{Serialize, Deserialize};

// layout overview:
// Explicit nulls still have a slot in the vector but shouldn't be read.
// {foo: {bar: 5}}
// {foo: {bar: null}}
// {foo: null}
// foo -> object{nulls=[f,f,t], size=[1,1,0]}
// foo.bar -> number{nulls=[f,t], vals=[5,0]}
//
// {foo: [{bar: 5}]}
// foo -> array{nulls=[f], size=[1]}
// foo.[] -> object{nulls=[f], size=[1]}
// foo.[].bar -> number{nulls=[f], vals=[5]}
//
// {foo: [[{bar: 5}]]
// foo -> array{nulls=[f], size=[1]}
// foo.[] -> array{nulls=[f], size=[1]}
// foo.[].[] -> object{nulls=[f], size=[1]}
// foo.[].[].bar -> number{nulls=[f], vals=[5]}
//
// {foo: {bar: 5}}
// {foo: [{bar: 5}]}
// foo -> union{nulls=[f,f] vals=[object{size=1}, array{size=1}]}
// foo.bar -> number{nulls=[f], vals=[5]}
// foo.[] -> object{nulls=[t], size=[1]}
// foo.[].bar -> number{nulls=[f], vals=[5]}

// Ideas:
// * Swap out offset array with a smarter data structure, can we use RLE?

/// A path to a json node
pub type Path = Vec<PathComponent>;

/// A segment of a path to a json node.
/// Array offsets aren't stored with the
#[derive(Debug, Eq, PartialEq, Clone, Ord, PartialOrd, Deserialize, Serialize)]
pub enum PathComponent {
    Key(String),
    Array,
}

/// A chunk of data that's been serialized in one go.
/// Indexes within the data are all stripe local,
#[derive(Debug, Serialize, Deserialize)]
pub struct Stripe {
    columns: BTreeMap<Path, Column>,
    count: usize,
}

impl Stripe {
    /// Create a new stripe to write data into
    pub fn new() -> Self {
        Stripe {
            columns: BTreeMap::new(),
            count: 0,
        }
    }

    /// Push a datum into the stripe
    pub fn push_datum(&mut self, datum: &Datum) {
        self.push_datum_at_path(datum, &[], &[self.count]);
        self.count += 1;
    }

    /// Get a column at a given path
    pub fn get_column(&self, path: &[PathComponent]) -> Option<&Column> {
        self.columns.get(path)
    }

    /// Write datum into a given column, will recursively write nested values
    fn push_datum_at_path(&mut self, datum: &Datum, path: &[PathComponent], indexes: &[usize]) {
        if datum.is_missing() {
            return;
        }
        if !self.columns.contains_key(path) {
            self.columns
                .insert(path.to_vec(), Column::new(indexes.len()));
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
