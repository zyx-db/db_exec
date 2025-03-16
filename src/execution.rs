use std::{any::Any, collections::BTreeMap, fmt, marker::PhantomData};

#[derive(Clone, Debug)]
pub enum Type {
    Str,
    U32,
    I32,
    Bool,
}

pub struct Row {
    data: Vec<Box<dyn Any>>,
}

impl Row {
    pub fn get<T: 'static>(&self, index: usize) -> Option<&T> 
    {
        self.data.get(index).and_then(|v| v.downcast_ref::<T>())
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = &dyn Any> {
        self.data.iter().map(|v| &**v)
    }
}

#[derive(Clone)]
pub struct RowSchema {
    data_types: Vec<Type>
}

impl RowSchema {
    pub fn new(data_types: Vec<Type>) -> Self {
        RowSchema { data_types }
    }

    pub fn make_row(&self, values: Vec<Box<dyn Any>>) -> Result<Row, String> {
        if values.len() != self.data_types.len() {
            return Err(format!(
                "expected {} values but got {}",
                self.data_types.len(),
                values.len()
            ))
        }
        for (expected_t, v) in self.data_types.iter().zip(values.iter()) {
            use Type::*;
            match expected_t {
                U32 => {
                    if v.downcast_ref::<u32>().is_none(){
                        return Err("type mismatch, expected u32".to_string());
                    }
                }
                I32 => {
                    if v.downcast_ref::<i32>().is_none(){
                        return Err("type mismatch, expected i32".to_string());
                    }
                }
                Str => {
                    if v.downcast_ref::<String>().is_none(){
                        return Err("type mismatch, expected String".to_string());
                    }
                }
                Bool => {
                    if v.downcast_ref::<bool>().is_none(){
                        return Err("type mismatch, expected bool".to_string());
                    }
                }
            }
        }

        Ok(Row {data: values})
    }

    pub fn print(&self, r: &Row) -> String {
        let row_len = r.len();
        let output: String = r.iter()
            .enumerate()
            .map(|(i, v)| {
                let t = &self.data_types[i];
                let trailing = {
                    if i < row_len - 1 {" - "}
                    else {""}
                };
                use Type::*;
                let value_str = match t{
                    U32 => {
                        format!("{}", v.downcast_ref::<u32>().unwrap())
                    }
                    I32 => {
                        format!("{}", v.downcast_ref::<i32>().unwrap())
                    }
                    Bool => {
                        format!("{}", v.downcast_ref::<bool>().unwrap())
                    }
                    Str => {
                        format!("{}", v.downcast_ref::<String>().unwrap())
                    }
                };
                format!("field {}: {} (type{:?}){}", i, value_str, t, trailing)
            })
            .collect(); 

        output
    }
}

pub struct Scan<'a, T> 
where
    T: Ord
{
    iterator: std::collections::btree_map::Iter<'a, T, Row>,
}

impl<'a, T> Scan<'a, T> 
where
    T: Ord
{
    pub fn new(map: &'a BTreeMap<T, Row>) -> Self {
        Scan {
            iterator: map.iter(),
        }
    }
}

impl<'a, T> Iterator for Scan<'a, T> 
where
    T: Ord
{
    type Item = &'a Row;

    fn next(&mut self) -> Option<Self::Item> {
        self.iterator.next().map(|(_, row)| row)
    }
}

pub struct FilterIterator<'a, I, F>
where
    I: Iterator<Item = &'a Row>,
    F: FnMut(&Row) -> bool,
{
    iter: I,
    predicate: F,
}

impl<'a, I, F> FilterIterator<'a, I, F>
where
    I: Iterator<Item = &'a Row>,
    F: FnMut(&Row) -> bool,
{
    pub fn new(iter: I, predicate: F) -> Self {
        FilterIterator { iter, predicate }
    }
}

impl<'a, I, F> Iterator for FilterIterator<'a, I, F>
where
    I: Iterator<Item = &'a Row>,
    F: FnMut(&Row) -> bool,
{
    type Item = &'a Row;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(row) = self.iter.next() {
            if (self.predicate)(row) {
                return Some(row);
            }
        }
        None
    }
}

pub struct JoinSchema {
    source_iter: Vec<usize>,
    iter_index: Vec<usize>,
    output_schema: RowSchema
}

impl JoinSchema {
    pub fn generate_from_rows(&self, rows: Vec<&Row>) -> Row{
        let mut data: Vec<Box<dyn Any>> = Vec::new();
        for i in 0..self.source_iter.len(){
            let source_idx = self.source_iter[i];
            let idx = self.iter_index[i];
            let t = &self.output_schema.data_types[i];

            let r = rows[source_idx];

            use Type::*;
            match t {
                U32 => {
                    let x = r.get::<u32>(idx).unwrap().clone();
                    data.push(Box::new(x));
                }
                I32 => {
                    let x = r.get::<i32>(idx).unwrap().clone();
                    data.push(Box::new(x));
                }
                Bool => {
                    let x = r.get::<bool>(idx).unwrap().clone();
                    data.push(Box::new(x));
                }
                Str => {
                    let x = r.get::<String>(idx).unwrap().clone();
                    data.push(Box::new(x));
                }
            }
        }

        return Row { data };
    }
}

pub struct HashJoinIterator<'a, I, T>
where
    I: Iterator<Item = &'a Row>,
    T: Ord + Clone + 'static,
{
    left: I,
    right: I,
    left_key_idx: usize,
    right_key_idx: usize,
    phantom: PhantomData<&'a T>
}

impl<'a, I, T> HashJoinIterator<'a, I, T>
where
    I: Iterator<Item = &'a Row>,
    T: Ord + Clone + 'static,
{
    pub fn new(left: I, right: I, left_key_idx: usize, right_key_idx: usize) -> Self {
        HashJoinIterator::<I, T> {
            left,
            right,
            left_key_idx,
            right_key_idx,
            phantom: PhantomData
        }
    }
}

impl<'a, I, T> Iterator for HashJoinIterator<'a, I, T>
where
    I: Iterator<Item = &'a Row>,
    T: Ord + Clone + 'a,
{
    type Item = &'a Row;

    fn next(&mut self) -> Option<Self::Item> {
        //let left_map: BTreeMap<T, &'a Row> = BTreeMap::new();
        let l = self.left_key_idx;
        let left_map: BTreeMap<T, &'a Row>= self
            .left
            .by_ref()
            .map(|row| {
                let key = row.get::<T>(l).unwrap().clone();
                (key, row)
            })
            .collect();
        //let left_map: BTreeMap<&'a T, &'a Row>= self
        //    .left
        //    .by_ref()
        //    .map(|row| {
        //        let r = row.clone();
        //        let key = r.get::<T>(l).unwrap().clone();
        //        (key, r)
        //    })
        //    .collect();

        None
    }
}

pub struct NestedJoinIterator<'a, I, T>
where
    I: Iterator<Item = &'a Row>,
    T: Ord + Clone + 'static,
{
    left: I,
    right: I,
    left_key_idx: usize,
    right_key_idx: usize,
    join_schema: JoinSchema,
    outputs: Vec<Row>,
    phantom: PhantomData<&'a T>,
}

impl<'a, I, T> NestedJoinIterator<'a, I, T>
where
    I: Iterator<Item = &'a Row>,
    T: Ord + Clone + 'static,
{
    pub fn new(left: I, right: I, left_key_idx: usize, right_key_idx: usize, join_schema: JoinSchema) -> Self {
        let outputs = Vec::new();
        NestedJoinIterator::<I, T> {
            left,
            right,
            left_key_idx,
            right_key_idx,
            join_schema,
            outputs,
            phantom: PhantomData
        }
    }
}

impl<'a, I, T> Iterator for NestedJoinIterator<'a, I, T>
where
    I: Iterator<Item = &'a Row>,
    T: Ord + Clone + 'a,
{
    type Item = &'a Row;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(outer_row) = self.left.next() {
            while let Some(inner_row) = self.right.next() {
                let key_left = outer_row.get::<T>(self.left_key_idx).unwrap();
                let key_right = inner_row.get::<T>(self.right_key_idx).unwrap();
                if key_left == key_right{
                    let x = vec![outer_row, inner_row];
                    let result = self.join_schema.generate_from_rows(x);
                    self.outputs.push(result);
                    //return Some(&outputs[outputs.len() - 1]);
                }
            }
        }

        None
    }
}

macro_rules! make_row {
    ($schema:expr, $($val:expr), *) => {
        $schema.make_row(vec![$(Box::new($val) as Box<dyn Any>), *])
    };
}
