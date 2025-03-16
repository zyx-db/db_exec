#[macro_use]
mod execution;
mod utils;

use execution::*;
use utils::*;

use sqlparser::dialect::SQLiteDialect;
use sqlparser::parser::Parser;
use std::collections::{BTreeMap, HashMap};
use std::any::Any;

fn main() {

    let schema = RowSchema::new(
            vec![Type::U32, Type::Str, Type::U32]
        );

    let mut tables: HashMap<String, BTreeMap<u32, Row>> = HashMap::new();

    let users = BTreeMap::from([
        (1, make_row!(schema, 1 as u32, "Dhiraj".to_string(), 20 as u32).unwrap()),
        (2, make_row!(schema, 2 as u32, "db".to_string(), 6 as u32).unwrap()),
        (3, make_row!(schema, 3 as u32, "bomma".to_string(),8 as u32).unwrap()),
        (4, make_row!(schema, 4 as u32, "test".to_string(),40 as u32).unwrap()),
        (5, make_row!(schema, 5 as u32, "hello".to_string(),10 as u32).unwrap()),
        (6, make_row!(schema, 6 as u32, "??".to_string(),220 as u32).unwrap()),
    ]);
    
    tables.insert("users".to_string(), users);

    let query = {
        FilterIterator::new(
            FilterIterator::new(
                Scan::new(
                    &tables[&("users".to_string())]
                ),
                |row| row.get::<u32>(0).unwrap() % 2 == 0
            ),
            |row| row.get::<u32>(2).unwrap() > &9
        )
    };

    let output_schema = schema.clone();

    for row in query {
        println!("{}", output_schema.print(row));
    }
}
