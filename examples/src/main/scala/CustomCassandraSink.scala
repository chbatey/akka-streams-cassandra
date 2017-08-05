import akka.Done
import akka.stream.scaladsl.Sink
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler}
import akka.stream.{Attributes, Inlet, SinkShape}
import com.datastax.driver.core.{BoundStatement, PreparedStatement, Session}

import scala.concurrent.{Future, Promise}

class CustomCassandraSink[T](ps: PreparedStatement, binder: (T, PreparedStatement) => BoundStatement)(implicit session: Session)
  extends GraphStageWithMaterializedValue[SinkShape[T], Future[Done]] {

  private val in: Inlet[T] = Inlet("CassandraSink")

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val promise = Promise[Done]()
    val logic = new GraphStageLogic(shape) {
      override def preStart(): Unit = {
        pull(in)
      }

      setHandler(in, new InHandler {
        def onPush(): Unit = {
          val t: T = grab(in)
          session.execute(binder(t, ps))
          pull(in)
        }
      })

      override def postStop(): Unit = {
        promise.success(Done)
        println("Finished")
      }
    }
    (logic, promise.future)
  }
  val shape: SinkShape[T] = SinkShape(in)
}

object CustomCassandraSink {
  def apply[T](ps: PreparedStatement, binder: (T, PreparedStatement) => BoundStatement)(implicit session: Session): Sink[T, Future[Done]] =
    Sink.fromGraph(new CustomCassandraSink[T](ps, binder))
}

