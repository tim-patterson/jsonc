use std::error::Error;
use std::time::Instant;
use jsonc::datum::Datum;
use jsonc::loader::load_json;
use jsonc::row_expr::avg_using_row_expr;

fn main() -> Result<(), Box<dyn Error>> {
    // https://datasets-documentation.s3.eu-west-3.amazonaws.com/kafka/github_all_columns.ndjson
    println!("Loading data");
    let start = Instant::now();
    let data = load_json("github_all_columns.ndjson")?;
    let duration = start.elapsed();
    println!("Loaded data in {duration:?}");

    perf_test(average_review_comments_handrolled_row, "hand rolled", &data);
    perf_test(avg_using_row_expr, "row expr", &data);
    Ok(())
}

fn perf_test(f: fn(&[Datum]) -> f64, label: &str, data: &[Datum]) {
    println!("Calculating average review comments using {label}");
    for _ in 0..10 {
        let start = Instant::now();
        let avg = f(&data);
        let duration = start.elapsed();
        println!("Calculated average of {avg} in {duration:?}");
    }
}



fn average_review_comments_handrolled_row(data: &[Datum]) -> f64 {
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
