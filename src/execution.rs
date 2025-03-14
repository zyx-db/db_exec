use std::collections::BTreeMap;

pub struct Row {
    pub id: u32,
    pub name: String,
    pub age: u8,
}

impl Row {
    pub fn new(id: u32, name: String, age: u8) -> Self {
        Row { id, name, age }
    }
}

pub struct Scan<'a> {
    iterator: std::collections::btree_map::Iter<'a, u32, Row>,
}

impl<'a> Scan<'a> {
    pub fn new(map: &'a BTreeMap<u32, Row>) -> Self {
        Scan { iterator: map.iter() }
    }
}

impl<'a> Iterator for Scan<'a> {
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

pub struct JoinIterator<'a, I>
where
    I: Iterator<Item = &'a Row>,
{
    left: I,
    right: I
}

impl<'a, I> JoinIterator<'a, I>
where
    I: Iterator<Item = &'a Row>,
{
    pub fn new(left: I, right: I) -> Self {
        JoinIterator {left, right}
    }
}

impl<'a, I> Iterator for JoinIterator<'a, I>
where
    I: Iterator<Item = &'a Row>,
{
    type Item = &'a Row;

    fn next(&mut self) -> Option<Self::Item> {
        let left_map: BTreeMap<u32, &Row> = self
            .left
            .by_ref()
            .map(|row| (row.id.clone(), row))
            .collect();

        None
    }
}
