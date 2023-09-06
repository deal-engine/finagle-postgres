package io.getquill

import scala.util.Try as ScalaTry

import cats.effect.{IO, Resource}
import com.twitter.finagle.postgres.PostgresClient
import com.twitter.util.{Await, Future, Try as TwitterTry, Throw}
import weaver.*
import skunk.*
import natchez.Trace.Implicits.noop

object UtilsSkunk {
  def getClient: Option[PostgresClient] =
    for {
      host <- sys.env.get("PG_HOST")
      port = sys.env
        .get("PG_PORT")
        .flatMap { x =>
          ScalaTry { x.toInt }.toOption
        }
        .getOrElse(5432)
      user <- sys.env.get("PG_USER")
      password = sys.env.get("PG_PASSWORD")
      dbname <- sys.env.get("PG_DBNAME")
    } yield {
      new finagle_postgres.skunk.PostgresClientImpl(
        Session.pooled(
          host = host,
          port = port,
          user = user,
          database = dbname,
          password = password,
          max = 1
        )
      )
    }
}

object TestSkunk extends MutableTwitterFutureSuite {
  case class TableTestSkunk(id: Int, name: String)

  override type Res = FinaglePostgresContext[SnakeCase.type]
  override def sharedResource: Resource[IO, Res] =
    Resource.make {
      IO.fromOption(UtilsSkunk.getClient)(new Exception("Failed to load env")).map { client =>
        val ctx = new FinaglePostgresContext[SnakeCase.type](SnakeCase, client)
        val a = {
          import ctx.*
          for {
            _ <- ctx
              .run(sql"DROP TABLE IF EXISTS table_test_skunk".as[Insert[Unit]])
            _ <- ctx.run(
              sql"CREATE TABLE table_test_skunk (id integer, name text)"
                .as[Insert[Unit]]
            )
            _ <- ctx.run(
              sql"INSERT INTO table_test_skunk (id, name) VALUES (1, null)"
                .as[Insert[Unit]]
            )
          } yield ()
        }
        Await.result(a)
        ctx
      }
    } { client =>
      IO.blocking(client.close()).void
    }

  future("Can make insert and fetch") { ctx =>
    import ctx.*
    val results = ctx.transaction {
      for {
        _ <- ctx.run(
          query[TableTestSkunk].insertValue(TableTestSkunk(0, "hola"))
        )
        result0 <- ctx.run(query[TableTestSkunk].filter(_.id == 0))
        result1 <- ctx.run(query[TableTestSkunk].filter(_.id == 1))
        result2 <- ctx.run(query[TableTestSkunk]).liftToTry
        //_ = println(result2.throwable.getStackTrace.mkString("\n"))
        _ = println(result2)
      } yield Seq(result0, result1)
    }
    results.map { case Seq(result0, result1) =>
      expect(result0.contains(TableTestSkunk(0, "hola")))
      expect(result1.contains(TableTestSkunk(1, null)))
    }
  }

  trait SequentiallyComposableContainer[F[_]] extends cats.Monad[F]

  future("can have nested transactions") { ctx =>
    case class ValuesInTransaction(v: Seq[TableTestSkunk]) extends Exception(s"UnU: $v")
    import ctx.*
    for {
      _ <- ctx.run(
        query[TableTestSkunk].insertValue(TableTestSkunk(10, "10"))
      )
      values_down <- ctx.transaction {
        for {
          _ <- ctx.run(
            query[TableTestSkunk].insertValue(TableTestSkunk(20, "20"))
          )
          _ <- ctx.transaction {
            ctx.run(
              query[TableTestSkunk].insertValue(TableTestSkunk(30, "30"))
            )
          }
          values_in_transaction <- ctx.run(query[TableTestSkunk])
          _ <- Future.exception(ValuesInTransaction(values_in_transaction))
        } yield values_in_transaction
      }.liftToTry
      values_top <- ctx.run(query[TableTestSkunk])
    } yield {
      println(s"VT: $values_top")
      println(s"VD: $values_down")
      expect(values_down.isThrow)
      expect(values_down.throwable match {
        case ValuesInTransaction(v) => v.contains(TableTestSkunk(30, "30"))
        case _ => false
      })
      expect(values_top.length == 1)
      expect(values_top.contains(TableTestSkunk(10, "10")))
    }
  }
}
