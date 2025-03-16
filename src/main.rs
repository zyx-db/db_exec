#[macro_use]
mod execution;

use execution::*;

//use sqlparser::dialect::SQLiteDialect;
//use sqlparser::parser::Parser;
use std::collections::{BTreeMap, HashMap};
use std::any::Any;
use std::rc::Rc;

fn main() {

    let mut tables: HashMap<String, BTreeMap<u32, Rc<Row>>> = HashMap::new();

    let users_schema = RowSchema::new(
            vec![Type::U32, Type::Str, Type::U32]
        );

    let users = BTreeMap::from([
        (1, Rc::new(make_row!(users_schema, 1 as u32, "Dhiraj".to_string(), 20 as u32).unwrap())),
        (2, Rc::new(make_row!(users_schema, 2 as u32, "db".to_string(), 6 as u32).unwrap())),
        (3, Rc::new(make_row!(users_schema, 3 as u32, "bomma".to_string(),8 as u32).unwrap())),
        (4, Rc::new(make_row!(users_schema, 4 as u32, "test".to_string(),40 as u32).unwrap())),
        (5, Rc::new(make_row!(users_schema, 5 as u32, "hello".to_string(),10 as u32).unwrap())),
        (6, Rc::new(make_row!(users_schema, 6 as u32, "??".to_string(),220 as u32).unwrap())),
    ]);

    tables.insert("users".to_string(), users);

    let pets_schema = RowSchema::new(
            vec![Type::U32, Type::Str]
        );

    let pets = BTreeMap::from([
        (1, Rc::new(make_row!(pets_schema, 2 as u32, "pet 1".to_string()).unwrap())),
        (2, Rc::new(make_row!(pets_schema, 2 as u32, "pet 2".to_string()).unwrap())),
        (3, Rc::new(make_row!(pets_schema, 2 as u32, "pet 3".to_string()).unwrap()))
    ]);
    
    tables.insert("pets".to_string(), pets);

    // SELECT u.name, pet.name
    // FROM users u
    // JOIN pets p
    //  ON p.u_id = u.id
    // WHERE u.age < 10 
    let output_schema = RowSchema::new(vec![Type::Str, Type::Str]);
    let query = {
        NestedJoinIterator::new(
            FilterIterator::new(
                Scan::new(
                    &tables[&("users".to_string())]
                ),
                |row| row.get::<u32>(2).unwrap() < &1000
            ),
            Scan::new(
                &tables[&("pets".to_string())]
            ),
            0,
            0,
            Type::U32,
            JoinSchema::new(vec![0, 1], vec![1, 1], output_schema.clone())
        )
    };

    for row in query {
        println!("{}", output_schema.print(&row));
    }
}
