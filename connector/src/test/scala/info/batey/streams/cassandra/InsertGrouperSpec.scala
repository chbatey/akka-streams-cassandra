package info.batey.streams.cassandra

import org.scalatest.{ Matchers, WordSpec }

class InsertGrouperSpec extends WordSpec with Matchers {
  "A InsertGrouper" must {
    "Buffer the first query" in {
      val ig = InsertGrouper[Int, Int](_ => 1)

      ig.add(1) should equal(None)
    }

    "Return elements when batch size is met" in {
      val ig = InsertGrouper[Int, Int](_ => 1, 2)

      ig.add(1) should equal(None)
      ig.add(2) should equal(Option(Seq(1, 2)))
    }

    "Not return elements previously returned" in {
      val ig = InsertGrouper[Int, Int](_ => 1, 2)
      ig.add(1)
      ig.add(2)

      ig.add(3)
      ig.add(4) should equal(Option(Seq(3, 4)))
    }

    "Group elements based" in {
      val ig = InsertGrouper[(String, Int), String](t => t._1, 2)

      ig.add(("p1", 1)) should equal(None)
      ig.add(("p2", 1)) should equal(None)
      ig.add(("p1", 2)) should equal(Some(Seq(("p1", 1), ("p1", 2))))
      ig.add(("p2", 2)) should equal(Some(Seq(("p2", 1), ("p2", 2))))
    }

    "Max outstanding batches should return the largest batch" in {
      val ig = InsertGrouper[(String, Int), String](t => t._1, batchSize = 3, outstandingBatches = 2)

      ig.add(("p1", 1)) should equal(None)
      ig.add(("p1", 2)) should equal(None)
      ig.add(("p2", 1)) should equal(None)
      ig.add(("p3", 1)) should equal(Some(Seq(("p1", 1), ("p1", 2))))
      ig.add(("p1", 3)) should equal(Some(Seq(("p2", 1))))
    }

    "Flush should return any ourstanding groups" in {
      val ig = InsertGrouper[(String, Int), String](t => t._1, batchSize = 3, outstandingBatches = 2)

      ig.add(("p1", 1)) should equal(None)
      ig.add(("p2", 1)) should equal(None)

      ig.flush() should equal(Seq(Seq(("p1", 1)), Seq(("p2", 1))))
    }
  }
}
