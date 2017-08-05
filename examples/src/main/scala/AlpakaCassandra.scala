import java.nio.file.Paths

import akka.Done
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, IOResult}
import akka.stream.alpakka.cassandra.scaladsl.CassandraSink
import akka.stream.scaladsl.{FileIO, Framing, Sink, Source}
import akka.util.ByteString
import com.datastax.driver.core.{BoundStatement, Cluster, PreparedStatement}

import scala.concurrent.Future
import scala.util.{Failure, Success}
import java.lang.{Long => JLong}
import java.util.concurrent.TimeUnit

import com.google.common.base.Stopwatch

/*
1 - 139
 */

object AlpakaCassandra extends App {

  implicit val system  = ActorSystem("CassandraExample")
  implicit val mat = ActorMaterializer()

  import system.dispatcher

  val cluster = Cluster.builder().addContactPoint("localhost").build()
  implicit val session = cluster.connect("streaming")

  val insert = session.prepare("insert into streaming.words(word, ind) values (?, ?)")

  val binder: ((String, Long), PreparedStatement) => BoundStatement = (word: (String, Long), ps: PreparedStatement) => ps.bind(word._1, word._2.asInstanceOf[JLong])

  val sink: Sink[(String, Long), Future[Done]] = CassandraSink[(String, Long)](1, insert, binder)

  val lineByLineSource: Source[(String, Long), Future[IOResult]] = FileIO.fromPath(Paths.get("/usr/share/dict/linux.words"))
    .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 1024))
    .map(_.utf8String)
    .zipWithIndex

  val stopWatch = Stopwatch.createStarted()
  val process: Future[Done] = lineByLineSource.runWith(sink)

  process.flatMap(_ => system.terminate()) onComplete {
    case Success(_) =>
      stopWatch.stop()
      println(s"Finished. Took ${stopWatch.elapsed(TimeUnit.SECONDS)} seconds." )
      cluster.close()
    case Failure(ohDear) =>
      ohDear.printStackTrace(System.out)
  }
}
