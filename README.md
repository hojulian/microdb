# MicroDB

MicroDB is a distributed data store for providing highly available, isolated data access to existing R-DBMS or other data stores designed for a service-oriented architecture.

This is part of MicroDB research 2020-2021.

*Note: This is a toy implementation of the paper, it is not production ready!*

## Research

- [Talk](https://youtu.be/vE_--8faZkE)
- [Paper](https://search.library.brandeis.edu/permalink/01BRAND_INST/nmaao4/alma9923987793901921)

## Benchmark

- [Github](https://github.com/hojulian/mdb-bench)

## Use

```go
package main

import (
    "context"

    "github.com/hojulian/microdb/client"
    "github.com/hojulian/microdb/microdb"
)

func main() {
    // Read data origin configurations from file.
    if err := microdb.AddDataOriginFromCfg("./dataorigin.yaml"); err != nil {

    }

    // Start microdb client.
    c, err := client.Connect("127.0.0.1", "4222", "test-client", "test-cluster", "test_table")
    if err != nil {
        // ...
    }
    defer c.Close()

    c.Query(context.Background(), query, args...)
    c.Exec(context.Background(), query, args...)
}
```
