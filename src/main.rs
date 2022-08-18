use std::error::Error;
use std::time::Instant;
use jsonc::columnar::{ColumnData, PathComponent, Stripe};
use jsonc::datum::Datum;
use jsonc::loader::load_json;

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

    perf_test(average_review_comments_hand_rolled_row, "hand rolled row", &data);
    perf_test(average_review_comments_hand_rolled_column, "hand rolled columnar", &columnar);
    //println!("{columnar:?}");
    Ok(())
}

fn perf_test<T: ?Sized>(f: fn(&T) -> f64, label: &str, data: &T) {
    println!("Calculating average review comments using {label}");
    for _ in 0..10 {
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
            if let Some(Datum::Float(number)) = root.get("review_comments") {
                sum += *number;
                count += 1;
            }
        }
    }
    sum / count as f64
}

fn average_review_comments_hand_rolled_column(stripe: &Stripe) -> f64 {
    let mut sum = 0.0;
    let mut count = 0_u64;

    let path = vec![PathComponent::Key("review_comments".to_string())];
    if let Some(column) = stripe.get_column(&path) {
        if let ColumnData::Float(vec) = &column.data {
            for (idx, number) in vec.iter().enumerate() {
                if !column.null_map[idx] {
                    sum += *number;
                    count += 1;
                }
            }
        }
    }
    sum / count as f64
}
