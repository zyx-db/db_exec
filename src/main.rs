mod execution;

use execution::*;

use sqlparser::dialect::SQLiteDialect;
use sqlparser::parser::Parser;
use std::collections::BTreeMap;
use std::any::Any;

fn main() {

    let schema = RowSchema::new(
            vec![1, 1, 1],
            vec![0, 1, 2],
            vec![Type::U32, Type::Str, Type::U32]
        );

    let data = BTreeMap::from([
        (1 as u32, Row::new(vec![Box::new(1 as u32), Box::new("Dhiraj".to_string()), Box::new(20 as u32)])),
        (2 as u32, Row::new(vec![Box::new(2 as u32), Box::new("db".to_string()), Box::new(6 as u32)])),
        (3 as u32, Row::new(vec![Box::new(3 as u32), Box::new("bomma".to_string()), Box::new(8 as u32)])),
        (4 as u32, Row::new(vec![Box::new(4 as u32), Box::new("test".to_string()), Box::new(40 as u32)])),
        (5 as u32, Row::new(vec![Box::new(5 as u32), Box::new("hello".to_string()), Box::new(10 as u32)])),
        (6 as u32, Row::new(vec![Box::new(6 as u32), Box::new("??".to_string()), Box::new(220 as u32)])),
    ]);


    let scan: Scan<'_, u32> = Scan::new(&data);
    let f1 = FilterIterator::new(scan, |row| row.get::<u32>(0).unwrap() % 2 == 0);
    let filter = FilterIterator::new(f1, |row| row.get::<u32>(2).unwrap() > &9);

    let output_schema = schema.clone();

    for row in filter {
        println!("{}", output_schema.print(row));
    }
}
