package io.getquill.context.twitter

import cats.Functor
import cats.effect.kernel.Resource
import com.github.jasync.sql.db.{Connection, QueryResult, RowData}
import com.github.jasync.sql.db.pool.ConnectionPool
import com.github.jasync.sql.db.postgresql.PostgreSQLConnection
import com.twitter.concurrent.{AsyncMutex, Txs}
import com.twitter.util.{Future, Local, Promise, Return, Throw}
import org.slf4j.Logger

import java.util.concurrent.{
  CompletableFuture,
  CountDownLatch,
  RejectedExecutionException
}
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

  implicit def completableFutureToTwitter[A](
      completableFuture: => CompletableFuture[A]
  ): Future[A] = {
    val promise: Promise[A] = Promise[A]()
    completableFuture.handleAsync(new function.BiFunction[A, Throwable, Unit] {
      override def apply(t: A, u: Throwable): Unit =
        if (u != null) {
          u match {
            case e: java.util.concurrent.CompletionException =>
              promise.setException(e.getCause)
            case _ => promise.setException(u)
          }
        } else {
          promise.setValue(t)
        }
    })
    promise
  }

  implicit def twitterToCompletableFuture[A](
      future: => Future[A]
  ): CompletableFuture[A] = {
    val completableFuture: CompletableFuture[A] = new CompletableFuture
    future.respond {
      case Throw(e)  => completableFuture.completeExceptionally(e)
      case Return(r) => completableFuture.complete(r)
    }
    completableFuture
  }

  case class TwitterConnectionPool(
      pool: ConnectionPool[PostgreSQLConnection],
      logger: Logger
  ) {
    self =>
    def usingFreshConnectionFromPool[A](
        fn: TwitterConnection => Future[A]
    ): Future[A] =
      (for {
        _ <- Future.Unit
        conn <- pool.take()
        r <- fn(TwitterConnection(InternalConnection(conn))(logger)).liftToTry
        _ <- pool.giveBack(conn)
      } yield r).lowerFromTry
  }

  final case class QuillConcurrentException(hint: String)
      extends Exception(s"UNHANDLED RACE CONDITION. $hint")

  case class InternalConnection(private val connection: Connection) {
    private val innerMutex = new AsyncMutex()

    private[TwitterConnection] def use[A](
        fn: Connection => Future[A]
    )(
        log: Logger
    ): Future[A] = {
      Future
        .when(innerMutex.numPermitsAvailable <= 0) {
          Future {
            val error = QuillConcurrentException(hint =
              "AREN'T YOU TRYING TO RUN PARALLEL TRANSACTIONS?"
            )
            log.warn("Trying to use mutex in transactional connection.", error)
          }
        }
        .flatMap { _ =>
          innerMutex.acquireAndRun(fn(connection))
        }
    }
  }

  case class TwitterConnection(connection: InternalConnection)(logger: Logger) {
    def inTransaction[A](fn: TwitterConnection => Future[A]): Future[A] =
      connection.use(_.inTransaction({ conn =>
        fn(TwitterConnection(InternalConnection(conn))(logger))
      }))(logger)

    def sendQuery(sql: String): Future[QueryResult] =
      connection.use(c => completableFutureToTwitter(c.sendQuery(sql)))(logger)

    def sendPreparedStatement(
        sql: String,
        params: Seq[Any]
    ): Future[QueryResult] =
      connection.use(c =>
        completableFutureToTwitter(
          c.sendPreparedStatement(sql, params.asJava)
        )
      )(logger)

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
