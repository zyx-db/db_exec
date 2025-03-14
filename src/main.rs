use std::collections::BTreeMap;

enum Types {
    String,
    I32,
    Bool,
}

struct Row {
    id: u32,
    name: String,
    age: u8,
}

impl Row {
    pub fn new(id: u32, name: String, age: u8) -> Self {
        Row { id, name, age }
    }
}

struct Scan<'a> {
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

struct FilterIterator<'a, I, F>
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
    fn new(iter: I, predicate: F) -> Self {
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
}
