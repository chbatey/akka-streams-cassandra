package info.batey.streams.cassandra

import akka.Done
import akka.stream._
import akka.stream.scaladsl.Sink
import akka.stream.stage._
import com.datastax.driver.core.BatchStatement.Type
import com.datastax.driver.core._

import scala.concurrent._
import scala.reflect.ClassTag
import scala.util.{ Failure, Success }

class BatchingCassandraSink[T: ClassTag, A: ClassTag](
  ps: PreparedStatement,
  binder: (T, PreparedStatement) => BoundStatement,
  grouper: (T => A),
  settings: BatchCassandraSinkSettings)(implicit session: Session, ex: ExecutionContext)
  extends GraphStageWithMaterializedValue[SinkShape[T], Future[Done]] {

  private val in: Inlet[T] = Inlet("CassandraSink")
  val shape: SinkShape[T] = SinkShape(in)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val promise = Promise[Done]()
    (new CassandraSinkGraphLogic[T, A](settings, ps, binder, in, grouper, shape, promise), promise.future)
  }
}

private[cassandra] class CassandraSinkGraphLogic[T, A](
  settings: BatchCassandraSinkSettings,
  ps: PreparedStatement,
  binder: (T, PreparedStatement) => BoundStatement,
  in: Inlet[T],
  grouper: T => A, shape: SinkShape[T],
  promise: Promise[Done])(implicit session: Session, ec: ExecutionContext)
  extends GraphStageLogic(shape) {

  val insertGrouper: InsertGrouper[T, A] = InsertGrouper[T, A](grouper)
  var outstandingQueries = 0

  override def preStart(): Unit = {
    pull(in)
  }

  setHandler(in, new InHandler {

    override def onUpstreamFinish(): Unit = {
      // Honor max outstanding inserts?
      val outstandingInserts: Seq[Future[ResultSet]] = insertGrouper.flush().map(executeBatch)
      Future.sequence(outstandingInserts)
        .onComplete {
          case Success(_) => promise.success(Done)
          case Failure(ex) => promise.tryFailure(ex)
        }
    }

    override def onUpstreamFailure(ex: Throwable): Unit = {
      promise.tryFailure(ex)
      super.onUpstreamFailure(ex)
    }

    def onPush(): Unit = {
      val work = insertGrouper.add(grab(in))
      work match {
        case Some(toInsert) =>
          outstandingQueries += 1
          val next: Future[ResultSet] = executeBatch(toInsert)
          val nextCallback: AsyncCallback[ResultSet] = getAsyncCallback((_: ResultSet) => {
            outstandingQueries -= 1
            maybePull()
          })

          next.onComplete {
            case Success(rs) => nextCallback.invoke(rs)
            case Failure(exp) => fail(exp)
          }

          maybePull()
        case None => maybePull()
      }
    }
  })

  private def fail(ex: Throwable): Unit = {
    failStage(ex)
    promise.tryFailure(ex)
  }

  private def maybePull(): Unit = {
    if (outstandingQueries < settings.maxOutstandingInserts) {
      pull(in)
    }
  }

  private def executeBatch(s: Seq[T]): Future[ResultSet] = {
    val batch = new BatchStatement(Type.UNLOGGED)
    s.foreach(el => batch.add(binder(el, ps)))
    val next: Future[ResultSet] = session.executeAsync(batch).asScala()
    next
  }
}

object BatchingCassandraSink {
  def apply[T: ClassTag, A: ClassTag](
    ps: PreparedStatement,
    binder: (T, PreparedStatement) => BoundStatement,
    grouper: (T => A),
    settings: BatchCassandraSinkSettings = BatchCassandraSinkSettings())(implicit session: Session, ec: ExecutionContext): Sink[T, Future[Done]] =
    Sink.fromGraph(new BatchingCassandraSink[T, A](ps, binder, grouper, settings))
}

