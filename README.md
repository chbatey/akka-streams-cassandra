# Batching Cassandra sink

An Akka streams sinks for Apache Cassandra.

This will be contributed so Alpakka once more mature. The existing sink uses `Flow[T].mapAsyncUnordered`
which is simple and works well for low throughput cases. This sink aims to increase throughput
using unlogged Cassandra batches with custom grouping at the expense of latency due to the sink buffering.

Batches are generally to be avoided however they can drastically increase throughput for bulk operations. For the use case
this sink was developed for a ~1000x throughput increase was achieved.

## Future features

* A grouper that reads the schema to automatically group by partition key. User must provide a function currently.
* Time limit for buffering

