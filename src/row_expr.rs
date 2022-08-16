use crate::datum::Datum;

pub fn avg_using_row_expr(data: &[Datum]) -> f64 {
    let mut expr = RowExpr::AggFunction(Box::new(AverageFunction { count: 0, sum: 0.0}),
                                        vec![RowExpr::Field("review_comments".to_string())].into_boxed_slice());
    for row in data {
        expr.evaluate_agg(row);
    }
    if let Datum::Float(f) = expr.agg_result() {
        f
    } else {
        panic!()
    }
}





pub enum RowExpr {
    // Grabs a field name
    Field(String),
    // Calls a function
    AggFunction(Box<dyn RowAggFunction>, Box<[RowExpr]>)
}

pub trait RowAggFunction {
    fn process(&mut self, input: &[Datum]);
    fn result(&self) -> Datum;
}

impl RowExpr {
    // Evaluates an expression in a row context
    pub fn evaluate(&mut self, row: &Datum) -> Datum {
        match self {
            RowExpr::Field(field_name) => {
                if let Datum::Object(obj) = row {
                    if let Some(child) = obj.get(field_name) {
                        child.clone()
                    } else {
                        Datum::Null
                    }
                } else {
                    Datum::Null
                }
            }
            RowExpr::AggFunction(_, _) => Datum::Null
        }
    }

    // feeds data into an aggregate function
    pub fn evaluate_agg(&mut self, row: &Datum) {
        if let RowExpr::AggFunction(function, input_exprs) = self {
            let inputs: Vec<_> = input_exprs.iter_mut().map(|expr| expr.evaluate(row)).collect();
            function.process(&inputs);
        }
    }

    pub fn agg_result(&mut self) -> Datum {
        if let RowExpr::AggFunction(function, _) = self {
            function.result()
        } else {
            panic!()
        }
    }
}

pub struct AverageFunction {
    sum: f64,
    count: u64
}

impl RowAggFunction for AverageFunction {
    fn process(&mut self, input: &[Datum]) {
        if let Datum::Float(f) = &input[0] {
            self.sum += *f;
            self.count += 1;
        }
    }

    fn result(&self) -> Datum {
        Datum::Float(self.sum / self.count as f64)
    }
}