# Database Lab-5

The design of lab5 is not difficult, but I don’t know much about the internal mechanism of java thread scheduling, so I spent a lot of time for debugging.

## Design Overview

- I leverage the `synchronized` in almost all functions to ensure the safety of multi-thread, and I found that I no longer need thread-safe data structures, because no variable will be accessed at the same time. On the other hand, `CocurrentHashMap` is too slow, so I replace all map with `HashMap`.

- I found that some test points took a long time, but in fact it is just a multi-threaded version of some previous test points. Considering that the previous test points did not consume a lot of time, and my `BufferPool` can only one thread will access at the same time, so I need to optimize the thread-switch and and try to ensure that all threads do more useful things when running instead of acquiring a lock but get fail.

- For ensure every variables will only be accessed by only one thread, java will take much time. My way to solve it is add `final` keyword in some variable.

- For maintaining the dirty page, we can sill use the dirty mark in page. But when need find all dirty pages related on specific `Transaction`, we have to scan all pages in buffer pool, so we can maintain a extra set for all `Transaction` to save all dirty pages. The structure will speed up the commit operation.

- About page lock, maintain the `readLocks` include the `Transaction` who want to read the page, and the  `writeLocks` who want to write the page. For any new request of reading/writing page from a `Transaction`, we can easily find the page lock and judge if the request can be accepted. Here are some details:

  - Size of `writeLocks` less that 1.
  - If `writeLocks` is empty, `readLocks` can contains many `Transaction`s.
  - If `writeLocks` is not empty, `readLocks` must be empty or it only contains one element which same as the element in `writeLocks`.

- We use two map, one map maintain all locks holds by one `Transaction`. Another map maintain page id to page lock.

- If a thread acquire a lock but fail first,  the second acquire must be fail. Because in my design, only one thread is running at any time. So we need use `wait` to release the ownership of `BufferPool`, or the thread will do nothing.

- When we complete one `Transaction`, if the `commit` is false, it means we need to recover all dirty pages, but note that we will not `evict` any dirty page to disk, so we can simply discard all dirty pages related to the Transaction. If `commit` the true, flush all pages. And then we need release all locks the `Transaction` holds. We scan all locks the `Transaction` holds, and delete the `Transaction` from the lock.

- Deadlock Check: wait and acquire, if acquire time exceed the timeout, we think deadlock happens.

- I meet a critical bug, when the Deadlock happen, we need to throw a `TransactionAbortedException`. But the exception may be caught in my incorrect code, such as

  ```java
  try {} catch (Exception e) { /*...*/ }
  ```

## API ChangeLog

- None

## Incomplete

- None
