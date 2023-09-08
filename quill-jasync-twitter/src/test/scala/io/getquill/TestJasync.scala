package io.getquill

import cats.effect.std.Backpressure

import scala.util.{Try => ScalaTry}
import cats.effect.{IO, Resource}
import com.github.jasync.sql.db.ConnectionPoolConfigurationBuilder
import com.github.jasync.sql.db.pool.ConnectionPool
import com.github.jasync.sql.db.postgresql.{
  PostgreSQLConnection,
  PostgreSQLConnectionBuilder
}
import com.twitter.util.{Await, Future, FuturePool, Throw, Try => TwitterTry}
import io.getquill.context.twitter.TwitterPostgresContext
import weaver.*

import java.util.concurrent.Executor

object UtilsJasync {
  def getClient: Option[ConnectionPool[PostgreSQLConnection]] =
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
      val config: ConnectionPoolConfigurationBuilder =
        new ConnectionPoolConfigurationBuilder()
      config.setHost(host)
      config.setPort(port)
      config.setUsername(user)
      password.foreach(config.setPassword)
      config.setDatabase(dbname)
      config.setMaxActiveConnections(1)
      config.setExecutionContext(new Executor {
        override def execute(command: Runnable): Unit =
          FuturePool.unboundedPool {
            command.run()
          }
      })
      println("sdksdmk")
      PostgreSQLConnectionBuilder.createConnectionPool(config)
    }
}

object TestJasync extends MutableTwitterFutureSuite {
  case class TableTestJasync(id: Int, name: String)

  override type Res = TwitterPostgresContext[SnakeCase.type]
  override def sharedResource: Resource[IO, Res] =
    Resource.make {
      IO.fromOption(UtilsJasync.getClient)(new Exception("Failed to load env"))
        .map { client =>
          val ctx =
            new TwitterPostgresContext[SnakeCase.type](SnakeCase, client)
          val a = {
            import ctx.*
            for {
              _ <- ctx
                .run(
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
}
