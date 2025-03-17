# learning about database execution!

hi! im db and rn im playing around in this repo to learn more about how databases execute queries.

## what i got working
right now, i can manually add rows to "tables", following a predefined table schema. from there i can manually set up my execution pipeline to emulate running a query.

for example running this query

```
SELECT u.name, pet.name
FROM users u
JOIN pets p
  ON p.u_id = u.id
WHERE u.age < 10
```

maps to this code, where i manually define what iterators i need, as well as how they should process the rows they get from their children

```rust
let output_schema = RowSchema::new(vec![Type::Str, Type::Str]);
let query = {
    NestedJoinIterator::new(
        FilterIterator::new(Scan::new(&tables[&("users".to_string())]), |row| {
            row.get::<u32>(2).unwrap() < &10
        }),
        Scan::new(&tables[&("pets".to_string())]),
        0,
        0,
        Type::U32,
        JoinSchema::new(vec![0, 1], vec![1, 1], output_schema.clone()),
    )
};
```

as you can see, i have to specify a lot right now!

## goals

so there are a few different things i want to try and accomplish:

### 1. convert logical plans to physical plans
right now i specify a physical plan (notice how i specify "NestedJoin", as opposed to just Join). I would ideally have my system decide what implementation to use for any given operation (ex: join, filter, scan, etc)

### 2. choose optimized operators based on table statistics
for certain operations like NestedLoopJoins, HashJoins, SortMergeJoins, the fastest one depends on the input. I should be able to look at stats about my input, and choose which is the best. I should also figure out which table should be on the left vs right.

### 3. parse SQL to form plans (stretch goal)
would be cool just providing SQL directly

### 4. connecting this set up to an actual storage engine to save records to disk (stretch goal)
I need to spend more time learning about how to serialize records to disk, and try to write my own BTree impl, so i dont see myself doing this anytime soon tbh.
