import java.nio.file.Paths
import java.util.concurrent.TimeUnit

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, IOResult, SinkShape}
import akka.stream.scaladsl._
import akka.stream.stage.GraphStage
import akka.util.ByteString
import com.datastax.driver.core.{BoundStatement, Cluster, PreparedStatement}
import com.google.common.base.Stopwatch
import java.lang.{Long => JLong}

import scala.concurrent.Future
import scala.io.StdIn
import scala.util.{Failure, Success}

object CassandraSinkExample extends App {

  implicit val system  = ActorSystem("CassandraExample")
  implicit val mat = ActorMaterializer()

  import system.dispatcher

  val cluster = Cluster.builder().addContactPoint("localhost").build()
  implicit val session = cluster.connect("streaming")

  val insert = session.prepare("insert into streaming.words(word, ind) values (?, ?)")

  val binder: ((String, Long), PreparedStatement) => BoundStatement = (word: (String, Long), ps: PreparedStatement) => ps.bind(word._1, word._2.asInstanceOf[JLong])


  val lineByLineSource: Source[(String, Long), Future[IOResult]] = FileIO.fromPath(Paths.get("/usr/share/dict/linux.words"))
    .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 1024))
    .map(_.utf8String)
    .zipWithIndex

  val sink: Sink[(String, Long), Future[Done]] = CustomCassandraSink[(String, Long)](insert, binder)

  val stopWatch = Stopwatch.createStarted()

  val process: Future[Done] = lineByLineSource.runWith(sink)

  process.onComplete {
    case m@_ =>
      stopWatch.stop()
      println(s"Took ${stopWatch.elapsed(TimeUnit.SECONDS)} seconds")
      println(m)
      system.terminate()
      cluster.close()
  }

}
