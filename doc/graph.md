# Graph

This document describes graph and the dependency deduction of it.

## Graph samples

Invalid:

```text
  ───────────────────────────────────────────────────────────`
  ↓                                                          |
begin -> w(x, v1) -> r(y, v1) -> commit                      |
                                  ↑ |                        |
                                  | |                        |
               ───────────────────  ↓                        |
              |                  begin -> r(x, v1) ...       |
              |                    |                         |
              |                    `───────────────────────  |
              |                                            ↓ |
             begin -> r(x, v0) -> w(x, v2) -> w(y, v1) -> commit
```

We can make it valid by removing a dependency, like:

```text
begin -> w(x, v1) -> r(y, v0) -> commit
                                  ↑ |
                                  | |
               ───────────────────  ↓
              |                  begin -> r(x, v1) ...
              |                    |
              |                    `────────────────────────
              |                                             ↓
             begin -> r(x, v0) -> w(x, v2) -> w(y, v1) -> commit
```

## Build a DAG

As we can change the write and expected read values in graph generated phase, we can force the graph to a DAG by this way. The mutations in a connection must occupy the realtime dependency, the other dependencies can be manually added after the mutations are generated. Actually we don't change a cyclic graph to a DAG but compose a DAG from a forest.

In this process, the difference of isolation levels between databases must be considered.

As the transaction is atomic, it should be treating as a whole, the following cases:

This is a value dependency graph, there is a cycle in it, but we cannot infer that this transaction is invalid. If `t2` starts before `t1`'s commit, then it's read action can get the old value.

```text
r(1, 1)
r(2, 1)
t1: begin -> w(1, 2) -> w(2, 2) -> commit
                ↑            |
                `─── RW ──`  |
                ── WW ────†──
                ↓         |
t2: begin -> w(2, 3) -> r(1, 1) -> commit
```

To change a value dependency graph into a transaction dependency graph, we make `WW` between commit actions and `RW` between begin and commit. Then we can infer that the transactions are valid.

```text
r(1, 1)
r(2, 1)
t1: begin -> w(1, 2) -> w(2, 2) -> commit
                                    ↑ |
       ──────────── RW ─────────────  |
      |                               WW
      |                               ↓
t2: begin -> w(2, 3) -> r(1, 1) -> commit
```
