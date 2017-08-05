package info.batey.streams.cassandra

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._

import scala.util.Random

@State(Scope.Thread)
class InsertGrouperBenchmark {

  @Param(Array("50", "100", "200"))
  var batchSize: Int = _

  @Param(Array("10", "100", "1000"))
  var outstandingBatches: Int = _

  @Param(Array("1", "10", "100"))
  var groups: Int = _

  // the range of the second element dictates how many groups there will be
  var grouper: InsertGrouper[(String, Int), Int] = _

  val random = new Random()

  @Setup
  def prepare() = {
    grouper = InsertGrouper(_._2, batchSize, outstandingBatches)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def insert(): Option[Seq[(String, Int)]] = {
    grouper.add(("cats and dogs", random.nextInt(groups)))
  }
}
