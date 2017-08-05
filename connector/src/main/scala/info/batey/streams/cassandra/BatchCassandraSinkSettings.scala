package info.batey.streams.cassandra

final case class BatchCassandraSinkSettings(
  maxOutstandingInserts: Int = 10,
  maxOutstandingBatches: Int = 100,
  maxBatchSize: Int = 1000) {
  def withMaxOutstandingBatches(i: Int): BatchCassandraSinkSettings =
    copy(maxOutstandingBatches = i)
  def withMaxBatchSize(i: Int): BatchCassandraSinkSettings =
    copy(maxBatchSize = i)
  def withMaxOutstandingInserts(i: Int): BatchCassandraSinkSettings =
    copy(maxOutstandingInserts = i)
}

