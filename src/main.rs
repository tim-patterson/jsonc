use jsonc::columnar::{ColumnData, PathComponent, Stripe};
use jsonc::datum::Datum;
use jsonc::loader::load_json;
use std::error::Error;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::time::Instant;

fn main() -> Result<(), Box<dyn Error>> {
    // https://datasets-documentation.s3.eu-west-3.amazonaws.com/kafka/github_all_columns.ndjson
    println!("Loading data");
    let start = Instant::now();
    let data = load_json("github_all_columns.ndjson")?;
    let duration = start.elapsed();
    println!("Loaded {} rows of data in {duration:?}", data.len());

    println!("Converting to columnar");
    let start = Instant::now();
    let mut columnar = Stripe::new();
    for datum in &data {
        columnar.push_datum(datum);
    }
    let duration = start.elapsed();
    println!("Columnarised data in {duration:?}");

    {
        let writer = BufWriter::new(File::create("json.columns")?);
        bincode::serialize_into(writer, &columnar)?;
    }
    println!("Loading columar data");
    let start = Instant::now();
    {
        let reader = BufReader::new(File::open("json.columns")?);
        columnar = bincode::deserialize_from(reader)?;
    }
    let duration = start.elapsed();
    println!("Loaded data in {duration:?}");

    perf_test(
        average_review_comments_hand_rolled_row,
        "hand rolled row",
        &data,
    );
    perf_test(
        average_review_comments_hand_rolled_column,
        "hand rolled columnar",
        &columnar,
    );
    //println!("{columnar:?}");
    Ok(())
}

fn perf_test<T: ?Sized>(f: fn(&T) -> f64, label: &str, data: &T) {
    println!("Calculating average review comments using {label}");
    for _ in 0..20 {
        let start = Instant::now();
        let avg = f(data);
        let duration = start.elapsed();
        println!("Calculated average of {avg} in {duration:?}");
    }
}

fn average_review_comments_hand_rolled_row(data: &[Datum]) -> f64 {
    let mut sum = 0.0;
    let mut count = 0_u64;

    for datum in data {
        if let Datum::Object(root) = datum {
            if let Some(datum) = root.get("review_comments") {
                if let Some(number) = datum.as_f64() {
                    sum += number;
                    count += 1;
                }
            }
        }
    }
    sum / count as f64
}

fn average_review_comments_hand_rolled_column(stripe: &Stripe) -> f64 {
    let path = vec![PathComponent::Key("review_comments".to_string())];
    let mut sum = 0.0;
    let mut count = 0_u64;
    if let Some(column) = stripe.get_column(&path) {
        if let ColumnData::Float(vec) = &column.data {
            for (number, null) in vec.iter().zip(column.null_map.iter()) {
                if !null {
                    sum += *number;
                    count += 1;
                }
            }
        }
        if let ColumnData::SmallInt(vec) = &column.data {
            for (number, null) in vec.iter().zip(column.null_map.iter()) {
                if !null {
                    sum += *number as f64;
                    count += 1;
                }
            }
        }
    }
    sum / count as f64
}
