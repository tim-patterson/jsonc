mod column;
use crate::columnar::column::Column;
pub use crate::columnar::column::ColumnData;
use crate::datum::Datum;
use std::collections::BTreeMap;
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
    Array,
}

/// A chunk of data that's been serialized in one go.
/// Indexes within the data are all stripe local
#[derive(Debug)]
pub struct Stripe {
    columns: BTreeMap<Path, Column>,
    count: usize,
}

impl Stripe {
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

    pub fn get_column(&self, path: &[PathComponent]) -> Option<&Column> {
        self.columns.get(path)
    }

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
