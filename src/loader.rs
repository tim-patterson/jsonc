use crate::datum::Datum;
use serde_json::Value;
use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

/// Loads data from a file into a vec of datum's, used for testing.
pub fn load_json<P: AsRef<Path>>(f: P) -> Result<Vec<Datum>, Box<dyn Error>> {
    let reader = BufReader::new(File::open(f)?);
    let mut results = Vec::new();

    //let mut c =0;

    for line in reader.lines() {
        let value: Value = serde_json::from_str(&line?)?;
        results.push(convert_from_value(value));
        // c += 1;
        // if c > 100 {
        //     break;
        // }
    }

    Ok(results)
}

/// Converts from serde value into our datum format
fn convert_from_value(val: Value) -> Datum {
    match val {
        Value::String(s) => Datum::String(s),
        Value::Number(n) => {
            if let Some(int) = n.as_i64() {
                if i8::MIN as i64 <= int && int <= i8::MAX as i64 {
                    return Datum::TinyInt(int as i8);
                } else if i16::MIN as i64 <= int && int <= i16::MAX as i64 {
                    return Datum::SmallInt(int as i16);
                }
            }
            if let Some(f) = n.as_f64() {
                Datum::Float(f)
            } else {
                Datum::Null
            }
        }
        Value::Null => Datum::Null,
        Value::Bool(b) => Datum::Bool(b),
        Value::Array(a) => Datum::Array(a.into_iter().map(convert_from_value).collect()),
        Value::Object(o) => Datum::Object(
            o.into_iter()
                .map(|(k, v)| (k, convert_from_value(v)))
                .collect(),
        ),
    }
}
