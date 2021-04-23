# Database Lab-2

The Lab took about two days. The implementation of B+ tree's operations consume me much time to understand its details.

## Design Overview

- HeapFile, HeapPage
  - Replace the static variable `PAGE_SIZE` by `BufferPool.getPageSize`.
  - HeapFile `insertTuple` find a empty page or create a new page first, then call the corresponding page `insertTuple` method, `deleteTuple` is same.
  - HeapPage `insertTuple`  find a empty slot then insert the tuple. `deleteTuple` get the slot from record id then delete it.
- BufferPool 
  - `insertTuple`  and `deleteTuple` just get the DB file and call corresponding method.
  - Use random strategy to implement the eviction algorithm.
- BTreeFile
  - `findLeafPage`: Search recursively in the n-ary tree by key value.
  - Split Page
    - Allocate a new page and move half of the element of the origin page to new page. 
    - Use the cutoff key to update the parent node and maintain the sibling and parent relation.
  - Stealing Operation
    - Move some elements until the two page have same elements number.
    - Delete the useless entry in internal page.
    - The remaining steps are the same as split.
  - Merge Page
    - Move all element in one page, mark another page is empty.
    - Delete the the origin cutoff key entry.
    - Maintain the sibling and parent relation.

## API ChangeLog

- Add `updatePage`, when a page is updated but not pass `getPage`, we need use the `updatePage` to update the page in buffer pool.

## Incomplete

- None

