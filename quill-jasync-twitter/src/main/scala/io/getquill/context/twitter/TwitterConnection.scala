package io.getquill.context.twitter

import cats.Functor
import cats.effect.kernel.Resource
import com.github.jasync.sql.db.{Connection, QueryResult, RowData}
import com.github.jasync.sql.db.pool.ConnectionPool
import com.github.jasync.sql.db.postgresql.PostgreSQLConnection
import com.twitter.util.{Future, Local, Promise, Return, Throw}

import java.util.concurrent.CompletableFuture
import java.util.function
import java.util.function.BiConsumer
import scala.jdk.CollectionConverters.*

object TwitterConnection {

  implicit def toKotlinFunction[T, R](
      f: T => R
  ): kotlin.jvm.functions.Function1[T, R] =
    new kotlin.jvm.functions.Function1[T, R] {
      override def invoke(t: T): R = f(t)
    }

  implicit val functorFuture: Functor[Future] = new Functor[Future] {
    override def map[A, B](fa: Future[A])(f: A => B): Future[B] =
      fa.map(f)
  }

  private val currentClient = new Local[TwitterConnection]

  def completableFutureToTwitter[A](
      completableFuture: => CompletableFuture[A]
  ): Future[A] = {
    val promise: Promise[A] = Promise[A]()
    completableFuture.handleAsync(new function.BiFunction[A, Throwable, Unit] {
      override def apply(t: A, u: Throwable): Unit =
        if (u != null) {
          promise.setException(u)
        } else {
          promise.setValue(t)
        }
    })
    promise
  }

  def twitterToCompletableFuture[A](
      future: => Future[A]
  ): CompletableFuture[A] = {
    val completableFuture: CompletableFuture[A] = new CompletableFuture
    future.respond {
      case Throw(e)  => completableFuture.completeExceptionally(e)
      case Return(r) => completableFuture.complete(r)
    }
    completableFuture
  }

  case class TwitterConnection(connection: Connection) {
    def withConnection[A](fn: TwitterConnection => Future[A]): Future[A] = {
      currentClient() match {
        case Some(value) => fn(value)
        case None        => fn(TwitterConnection(connection))
      }
    }

    def withTransaction[A](fn: TwitterConnection => Future[A]): Future[A] =
      currentClient() match {
        case Some(value) => fn(value) // We are in a transaction already
        case None =>
          completableFutureToTwitter {
            connection.inTransaction({ newConnection =>
              twitterToCompletableFuture {
                currentClient.let(TwitterConnection(newConnection)) {
                  withConnection(fn)
                }
              }
            })
          }
      }

    def sendQuery(sql: String): Future[QueryResult] =
      completableFutureToTwitter(connection.sendQuery(sql))

    def sendPreparedStatement(
        sql: String,
        params: Seq[Any]
    ): Future[QueryResult] =
      completableFutureToTwitter(
        connection.sendPreparedStatement(sql, params.asJava)
      )

    def prepareAndQuery[T](sql: String, params: Seq[Any])(
        f: RowData => T
    ): Future[Seq[T]] = {
      sendPreparedStatement(sql, params).map(_.getRows.asScala.toSeq.map(f))
    }

    /*
     * Issue a single, prepared arbitrary query without an expected result set, and provide the affected row count
     */
    def prepareAndExecute(sql: String, params: Seq[Any]): Future[Long] = {
      sendPreparedStatement(sql, params).map(_.getRowsAffected)
    }

    def execute(sql: String): Future[Long] = {
      sendQuery(sql).map(_.getRowsAffected)
    }
  }

}
