package com.twitter.finagle.postgres

import cats.effect.{IO, Resource}
import com.twitter.finagle.Postgres
import io.getquill.{FinaglePostgresContext, Query, SnakeCase}
import weaver.MutableTwitterFutureSuite

object Utils {
  def getClient: IO[PostgresClientImpl] =
    getConcurrentClient(maxConcurrency = 1)

  def getConcurrentClient(maxConcurrency: Int): IO[PostgresClientImpl] = IO
    .fromOption(for {
      hostPort <- sys.env.get("PG_HOST_PORT")
      user <- sys.env.get("PG_USER")
      password = sys.env.get("PG_PASSWORD")
      dbname <- sys.env.get("PG_DBNAME")
      useSsl = sys.env.getOrElse("USE_PG_SSL", "0") == "1"
      sslHost = sys.env.get("PG_SSL_HOST")
    } yield {
      val client = Postgres
        .Client()
        .withCredentials(user, password)
        .database(dbname)
        .withSessionPool
        .maxSize(maxConcurrency)
        .conditionally(
          useSsl,
          c => sslHost.fold(c.withTransport.tls)(c.withTransport.tls)
        )
        .newRichClient(hostPort)

      IO.blocking {
        while (!client.isAvailable) {}
        client
      }
    })(new Exception("Cant has confgs"))
    .flatten
}

object TransactionalTests extends MutableTwitterFutureSuite {
  override type Res = FinaglePostgresContext[SnakeCase.type]

  override def sharedResource: Resource[IO, Res] =
    Resource.make {
      Utils.getClient.map { client =>
        new FinaglePostgresContext[SnakeCase.type](SnakeCase, client)
      }
    } { ctx => IO.blocking(ctx.close()) }

  future("parallel connections work") { ctx =>
    import ctx.*
    val first = ctx.run(
      sql"""SELECT 1 FROM (SELECT pg_sleep(4)) AS subquery""".as[Query[Int]]
    )

    val second = ctx.transaction {
      val second_1 = ctx.run(
        sql"""SELECT 1 FROM (SELECT pg_sleep(4)) AS subquery""".as[Query[Int]]
      )
      val second_2 = ctx.run(
        sql"""SELECT 1 FROM (SELECT pg_sleep(4)) AS subquery""".as[Query[Int]]
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
