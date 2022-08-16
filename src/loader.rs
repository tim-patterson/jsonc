use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use serde_json::Value;
use crate::datum::Datum;

pub fn load_json<P: AsRef<Path>>(f: P) -> Result<Vec<Datum>,Box<dyn Error>> {
    let reader = BufReader::new(File::open(f)?);
    let mut results = Vec::new();

    let mut lines = reader.lines();
    while let Some(line) = lines.next() {
        let value: Value = serde_json::from_str(&line?)?;
        results.push(convert_from_value(value));
    }

    Ok(results)
}

fn convert_from_value(val: Value) -> Datum {
    match val {
        Value::String(s) => Datum::String(s),
        Value::Number(n) => if let Some(f) = n.as_f64() {
            Datum::Float(f)
        } else {
            Datum::Null
        },
        Value::Null => Datum::Null,
        Value::Bool(b) => Datum::Bool(b),
        Value::Array(a) => Datum::Array(a.into_iter().map(convert_from_value).collect()),
        Value::Object(o) => Datum::Object(o.into_iter()
            .map(|(k, v)| (k, convert_from_value(v))).collect())
    }
}