mod execution;

use execution::*;

use std::collections::BTreeMap;
use sqlparser::dialect::SQLiteDialect;
use sqlparser::parser::Parser;

fn main() {
    let mut data = BTreeMap::new();
    let names_ages = vec![
        ("Dhiraj".to_string(), 20),
        ("db".to_string(), 6),
        ("bomma".to_string(), 8),
        ("test".to_string(), 40),
        ("hello".to_string(), 10),
        ("??".to_string(), 220),
    ];

    for (i, (n, a)) in names_ages.into_iter().enumerate() {
        data.insert(i as u32, Row::new(i as u32, n, a));
    }

    let scan = Scan::new(&data);
    let f1 = FilterIterator::new(scan, |row| row.id % 2 == 0);
    let filter = FilterIterator::new(f1, |row| row.age > 9);

    for row in filter {
        println!("Filtered Row: {} - {} - {}", row.id, row.name, row.age);
    }

    let dialect = SQLiteDialect {}; // or AnsiDialect

    let sql = "SELECT * \
               FROM generic \
               WHERE id % 2 == 0 \
               AND age > 9";

    let ast = Parser::parse_sql(&dialect, sql).unwrap();

    println!("AST: {:?}", ast);
}
