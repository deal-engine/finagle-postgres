package io.getquill

import cats.effect.std.Backpressure

import scala.util.Try as ScalaTry
import cats.effect.{IO, Resource}
import com.twitter.util.{Await, Future, FuturePool, Throw, Try as TwitterTry}
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import io.getquill.context.twitter.{PostgresTwitterJdbcContext, TwitterJdbcContext}
import weaver.*

import java.io.Closeable
import java.util.concurrent.Executor
import javax.sql.DataSource

object UtilsJasync {

  val logT = Logger.apply("JasyncQuill")

  def getClient: Option[HikariDataSource] =
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
      // Config
      // HikariDataSource ds = new HikariDataSource();
      val hc: HikariConfig = new HikariConfig()

      hc.setUsername(user)
      password.foreach(hc.setPassword)
      hc.setJdbcUrl(s"jdbc:postgresql://$host:$port/$dbname")
      hc.setMaximumPoolSize(10)

      new HikariDataSource(hc)
    }
}

object TestJDBC extends MutableTwitterFutureSuite {
  case class TableTestJasync(id: Int, name: String)

  override type Res = TwitterJdbcContext[PostgresDialect, SnakeCase.type]
  override def sharedResource: Resource[IO, Res] =
    Resource.make {
      IO.fromOption(UtilsJasync.getClient)(new Exception("Failed to load env"))
        .map { client =>
          val ctx =
            new PostgresTwitterJdbcContext[SnakeCase.type](SnakeCase, client)
          val a = {
            import ctx.*
            for {
              _ <- ctx.run(
                  sql"DROP TABLE IF EXISTS table_test_jasync".as[Insert[Unit]]
                )
              _ <- ctx.run(
                sql"CREATE TABLE table_test_jasync (id integer, name text)"
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
          query[TableTestJasync].insertValue(TableTestJasync(0, "hola"))
        )
        result0 <- ctx.run(query[TableTestJasync].filter(_.id == 0))
        result1 <- ctx.run(query[TableTestJasync].filter(_.id == 1))
        result2 <- ctx.run(query[TableTestJasync]).liftToTry
        // _ = println(result2.throwable.getStackTrace.mkString("\n"))
        _ = println(result2)
      } yield Seq(result0, result1)
    }
    results.map { case Seq(result0, result1) =>
      expect(result0.contains(TableTestJasync(0, "hola")))
    }
  }

  trait SequentiallyComposableContainer[F[_]] extends cats.Monad[F]

  future("can have nested transactions") { ctx =>
    case class ValuesInTransaction(v: Seq[TableTestJasync])
        extends Exception(s"UnU: $v")
    import ctx.*
    for {
      _ <- ctx.run(
        query[TableTestJasync].insertValue(TableTestJasync(10, "10"))
      )
      values_down <- ctx.transaction {
        for {
          _ <- ctx.run(
            query[TableTestJasync].insertValue(TableTestJasync(20, "20"))
          )
          _ <- ctx.transaction {
            ctx.run(
              query[TableTestJasync].insertValue(TableTestJasync(30, "30"))
            )
          }
          values_in_transaction <- ctx.run(query[TableTestJasync])
          _ <- Future.exception(ValuesInTransaction(values_in_transaction))
        } yield values_in_transaction
      }.liftToTry
      values_top <- ctx.run(query[TableTestJasync])
    } yield {
      println(s"VT: $values_top")
      println(s"VD: $values_down")
      expect(values_down.isThrow) &&
      expect(values_down.throwable match {
        case ValuesInTransaction(v) =>
          println(v)
          v.contains(TableTestJasync(30, "30"))
        case _ => false
      }) &&
      expect(values_top.contains(TableTestJasync(10, "10"))) &&
      expect(!values_top.contains(TableTestJasync(20, "30"))) &&
      expect(!values_top.contains(TableTestJasync(20, "30")))
    }
  }

  future("parallel connections work") { ctx =>
    import ctx.*
    val first = ctx.run(
      sql"""SELECT 1 FROM (SELECT pg_sleep(2)) AS subquery""".as[Query[Int]]
    )

    val second = ctx.transaction {
      val second_1 = ctx.run(
        sql"""SELECT 1 FROM (SELECT pg_sleep(2)) AS subquery""".as[Query[Int]]
      )
      val second_2 = ctx.run(
        sql"""SELECT 1 FROM (SELECT pg_sleep(2)) AS subquery""".as[Query[Int]]
      )
      for {
        _ <- second_1
        _ <- second_2
      } yield ()
    }

    for {
      _ <- first
      _ <- second
    } yield {
      expect(true)
    }
  }
}
