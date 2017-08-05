package info.batey.streams.cassandra

import scala.collection.mutable.ArrayBuffer

private[cassandra] class InsertGrouper[T, A](
  grouper: (T => A),
  batchSize: Int,
  outstandingBatches: Int) {
  var buffer = Map[A, ArrayBuffer[T]]()

  def add(t: T): Option[Seq[T]] = {
    val group = grouper(t)
    val slot: ArrayBuffer[T] = getSlot(group)
    slot += t
    addToBatch(group, slot)
      .orElse(checkOutstandingBatches)
  }

  def flush(): Seq[Seq[T]] = {
    val flushed = buffer.toList.map(_._2.toList)
    buffer = Map()
    flushed
  }

  private def checkOutstandingBatches(): Option[Seq[T]] = {
    if (buffer.size > outstandingBatches) {
      val (group, elements) = buffer.maxBy {
        case (_, b) => b.size
      }
      buffer -= group
      Some(elements.toList)
    } else {
      None
    }
  }

  private def addToBatch(group: A, slot: ArrayBuffer[T]): Option[Seq[T]] = {
    if (slot.size == batchSize) {
      val batch = Some(slot.toList)
      buffer -= group
      batch
    } else {
      None
    }
  }

  private def getSlot(group: A) = {
    val slot = buffer.getOrElse(group, {
      val ab = new ArrayBuffer[T]()
      buffer = buffer.updated(group, ab)
      ab
    })
    slot
  }

}

object InsertGrouper {
  def apply[T, A](
    grouper: (T => A),
    batchSize: Int = 100,
    outstandingBatches: Int = 10): InsertGrouper[T, A] = new InsertGrouper(grouper, batchSize, outstandingBatches)

}
