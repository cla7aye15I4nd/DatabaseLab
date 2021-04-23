# Database Lab-3

The Lab took about 3 days, some bug in lab1 and B+ tree trigger some unexcepted problem.

## Design Overview

- Filter: iterator in the child and use the `Predicate` to filter the element.
- Join: It can be consider a double loop，and the fetch next do one step in in the double loop, maintain the element in first loop to solve it.
- HashJoin: It is used when the operator is equal, we can build a hash table for one list, than we loop in another list, we do not need loop but just search in the hash table to find the match elements.
- StringAggregator: Maintain a hash table from `field` to `int`, the hash table count the number of element.
- IntegerAggregator:  `IntegerAggregator` need main more information include the sum, count because of the `AVG` operator.
- Aggregate: `Aggregate` is just a wrapper, when create the Aggregate, we can build the iterator for all elements in advance.
- Insert, Delete: The two operator is simple just use the iterator to insert or delete, but my iterator in heap page can not support simultaneous deletion and iteration. So I spend some time to change it.

## API ChangeLog

- No changes

## Incomplete

- None

