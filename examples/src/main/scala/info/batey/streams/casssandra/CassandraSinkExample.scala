package info.batey.streams.casssandra

import java.lang.{ Long => JLong }
import java.nio.file.Paths
import java.util.concurrent.TimeUnit

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import akka.util.ByteString
import com.datastax.driver.core.{ Cluster, PreparedStatement }
import com.google.common.base.Stopwatch
import info.batey.streams.cassandra.BatchingCassandraSink

import scala.concurrent.Future

object CassandraSinkExample extends App {

  implicit val system = ActorSystem("CassandraExample")
  implicit val mat = ActorMaterializer()
  implicit val cluster = Cluster.builder().addContactPoint("localhost").build()
  implicit val session = cluster.connect("streams")

  import system.dispatcher

  val insert = session.prepare("insert into streams.words(word, ind) values (?, ?)")
  val binder = (word: (String, Long), ps: PreparedStatement) => ps.bind(word._1, word._2.asInstanceOf[JLong])

  val lineByLineSource = FileIO.fromPath(Paths.get("/usr/share/dict/linux.words"))
    .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 1024))
    .map(_.utf8String)
    .zipWithIndex

  val sink = BatchingCassandraSink[(String, Long), Int](insert, binder, _ => 1)
  val stopWatch = Stopwatch.createStarted()
  val process: Future[Done] = lineByLineSource.runWith(sink)
  process.onComplete {
    case m @ _ =>
      stopWatch.stop()
      println(s"Took ${stopWatch.elapsed(TimeUnit.SECONDS)} seconds")
      println(m)
      system.terminate()
      cluster.close()
  }
}
