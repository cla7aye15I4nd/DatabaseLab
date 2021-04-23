# Database Lab-1

This Lab took about two days. What confuses me most is the implementation of various iterators. My way to solve these problems is to read the source code of the test point.

## Design Overview

- CataLog: Maintain two hashmap, one mapping from table id to table information, the other one mapping table name to table id.
- BufferPool: Define a ConcurrentHashMap to maintain the cache of the pages.
- HashCode(x, y) = exp * x + y.
- HeapPage: The iterator of heap page follows the same logic of function `readNextTuple` which has implemented.
- HeapFile: The iterator of heap file maintains current page and a tuple iterator.
- Other class has no special design, only need to find a function in the form of getXXX or constructor to know what variable needs to be defined. And read the java doc to get the function logic.

## API ChangeLog

No Changes in API.

## Incomplete 

- BufferPool.java: Implement the multi-thread accessing of BufferPool and page swap strategy.
- I’m not sure exactly what `hashcode` is used for, whether it requires different elements to have different hash values.



