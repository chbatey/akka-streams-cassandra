package info.batey.streams.cassandra

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.testkit.scaladsl.TestSource
import com.datastax.driver.core.{Cluster, PreparedStatement}
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import scala.collection.JavaConverters._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.Failure

class CassandraSinkSpec extends WordSpec
  with Matchers
  with ScalaFutures
  with BeforeAndAfterAll
  with BeforeAndAfterEach {

  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()

  import system.dispatcher

  implicit val session = Cluster.builder.addContactPoint("127.0.0.1").withPort(9042).build.connect()

  private val keyspaceName = "akka_stream_cassandra"

  override def beforeAll(): Unit = {
    session.execute(
      s"""
         |CREATE KEYSPACE IF NOT EXISTS $keyspaceName WITH replication = {
         |  'class': 'SimpleStrategy',
         |  'replication_factor': '1'
         |};
      """.stripMargin
    )
    session.execute(
      s"""
         |CREATE TABLE IF NOT EXISTS $keyspaceName.words (
         |    word text PRIMARY KEY,
         |    count int
         |);
      """.stripMargin
    )
  }

  override protected def beforeEach(): Unit = {
    session.execute(s"TRUNCATE $keyspaceName.words")
  }

  override def afterAll(): Unit = {
    session.execute(s"DROP KEYSPACE IF EXISTS $keyspaceName;")
    Await.result(system.terminate(), 5.seconds)
  }

  def fixtures(implicit ec: ExecutionContext) = {
    new {
      val source = Source(0 to 10).map(i => (i.toString, i: Integer))
      val ps = session.prepare("INSERT INTO akka_stream_cassandra.words(word, count) VALUES (?, ?)")
      val binder = (wordCount: (String, Integer), statement: PreparedStatement) => statement.bind(wordCount._1, wordCount._2)
      val sink: Sink[(String, Integer), Future[Done]] = BatchingCassandraSink[(String, Integer), Int](ps, binder, _ => 1)
    }
  }

  "A batching cassandra sink" must {
    "write to the table" in {
      val underTest = fixtures

      val result = underTest.source.runWith(underTest.sink)
      result.futureValue

      val rows = session.execute("select word, count from akka_stream_cassandra.words").all().asScala
        .map(row => (row.getString("word"), row.getInt("count")))
      rows.toSet should equal((0 to 10).map(i => (i.toString, i: Integer)).toSet)
    }

    "propagate failure in source" in {
      val underTest = fixtures
      val (probe, future: Future[Done]) = TestSource.probe[(String, Integer)].
        toMat(underTest.sink)(Keep.both)
        .run()

      probe.sendError(new RuntimeException("Oh noes"))

      Await.ready(future, 1 second)
      val Failure(ex) = future.value.get
      ex.getMessage should equal("Oh noes")
    }

    "propagate cassandra failure" in {
      //todo - test with scassandra?
    }
  }
}
