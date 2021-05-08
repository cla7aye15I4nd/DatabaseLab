# Database Lab-4

The lab is simple, it consumes me one day.

## Design Overview

- IntHistogram.java: Essentially what we have to do is to maintain a bucket and ask for its prefix sum. We can use a **Fenwick Tree** to maintain it.

- TableStats.java: Scan the tables and insert each field into histograms, then we can use these histograms to do estimation.

-  JoinOptimizer.java: According to formula and algorithm to estimate the time and result. In `estimateTableJoinCardinality`, it use the `pkey` flag to made a more detailed estimation. 

- Last task we just need to translate the pseudocode to java.

  ```
  1. j = set of join nodes
  2. for (i in 1...|j|):
  3.     for s in {all length i subsets of j}
  4.       bestPlan = {}
  5.       for s' in {all length d-1 subsets of s}
  6.            subplan = optjoin(s')
  7.            plan = best way to join (s-s') to subplan
  8.            if (cost(plan) < cost(bestPlan))
  9.               bestPlan = plan
  10.      optjoin(s) = bestPlan
  11. return optjoin(j)
  ```

## API ChangeLog

- Optimize the `instantiateJoin` in `joinOptimizer`, if the operator is equal, it will use hash join method.

  ```java
  if (p.getOperator().equals(Predicate.Op.EQUALS)) j = new HashEquiJoin(p,plan1,plan2);
  ```

## Incomplete

- My estimation is not very accurate, I just pass the tests.
