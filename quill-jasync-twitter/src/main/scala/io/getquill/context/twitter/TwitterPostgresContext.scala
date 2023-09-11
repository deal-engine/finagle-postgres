package io.getquill.context.twitter

import com.github.jasync.sql.db.RowData
import com.github.jasync.sql.db.pool.ConnectionPool
import com.github.jasync.sql.db.postgresql.PostgreSQLConnection
import com.twitter.util.{Future, Local}
import io.getquill.ReturnAction.{ReturnColumns, ReturnNothing, ReturnRecord}
import io.getquill.{NamingStrategy, PostgresDialect, ReturnAction}
import io.getquill.context.{Context, ContextVerbTranslate, ExecutionInfo}
import io.getquill.context.sql.SqlContext
import io.getquill.util.ContextLogger
import org.slf4j.Logger

import scala.util.Try

class TwitterPostgresContext[+N <: NamingStrategy](
    val naming: N,
    pool: ConnectionPool[PostgreSQLConnection],
    loggerslf4j: Logger
) extends Context[PostgresDialect, N]
    with ContextVerbTranslate
    with SqlContext[PostgresDialect, N]
    with Encoders
    with Decoders
    with UUIDStringEncoding {

  val idiom = PostgresDialect

  private val logger = ContextLogger(classOf[TwitterPostgresContext[_]])
  override type Session = Unit
  override type Runner = Unit

  override type PrepareRow = Seq[Any]
  override type ResultRow = RowData

  override type Result[T] = Future[T]
  override type RunQueryResult[T] = List[T]
  override type RunQuerySingleResult[T] = T
  override type RunActionResult = Long
  override type RunActionReturningResult[T] = T
  override type RunBatchActionResult = List[Long]
  override type RunBatchActionReturningResult[T] = List[T]

  override type NullChecker = TwitterPgNullChecker

  class TwitterPgNullChecker extends BaseNullChecker {
    override def apply(v1: Index, v2: ResultRow): Boolean = {
      v2.get(v1) == null
    }
  }

  implicit val nullChecker: NullChecker = new TwitterPgNullChecker()

  // override private[getquill] def prepareParams(statement: String, prepare: Prepare) = ???

  override def close =
    // Await.result(pool.disconnect(), Duration.Inf) // TO CHANGE
    ()

  override def probe(statement: String): Try[_] =
    Try {
      // Await.result(pool.sendQuery(sql), Duration.Inf) TODO
    }

  private val twitterClientLocal: Local[TwitterConnection.TwitterConnection] =
    new Local[TwitterConnection.TwitterConnection]

  private val twitterClientPool =
    TwitterConnection.TwitterConnectionPool(pool = pool, loggerslf4j)

  // Only create a client if none exists to allow nested transactions.
  def transaction[T](f: => Future[T]): Future[T] =
    withClient {
      _.inTransaction { txConn =>
        twitterClientLocal.let(txConn) { f }
      }
    }

  def executeQuery[T](
      sql: String,
      prepare: Prepare = identityPrepare,
      extractor: Extractor[T] = identityExtractor
  )(info: ExecutionInfo, dc: Runner): Future[List[T]] = {
    val (params, prepared) = prepare(Nil, ())
    logger.logQuery(sql, params)
    withClient(
      _.prepareAndQuery(sql, prepared)(extractor(_, ())).map(_.toList)
    )
  }

  def executeQuerySingle[T](
      sql: String,
      prepare: Prepare = identityPrepare,
      extractor: Extractor[T] = identityExtractor
  )(info: ExecutionInfo, dc: Runner): Future[T] =
    executeQuery(sql, prepare, extractor)(info, dc).map(
      handleSingleResult(sql, _)
    )

  def executeAction[T](
      sql: String,
      prepare: Prepare = identityPrepare,
      extractor: Extractor[T] = identityExtractor
  )(info: ExecutionInfo, dc: Runner): Future[Long] = {
    val (params, prepared) = prepare(Nil, ())
    logger.logQuery(sql, params)
    if (params.nonEmpty) {
      withClient(_.prepareAndExecute(sql, prepared))
    } else {
      withClient(_.execute(sql)).map(_ => 0)
    }
  }

  def executeBatchAction[B](
      groups: List[BatchGroup]
  )(info: ExecutionInfo, dc: Runner): Future[List[Long]] =
    Future
      .collect {
        groups.map { case BatchGroup(sql, prepare) =>
          prepare
            .foldLeft(Future.value(List.newBuilder[Long])) {
              case (acc, prepare) =>
                acc.flatMap { list =>
                  executeAction(sql, prepare)(info, dc).map(list += _)
                }
            }
            .map(_.result())
        }
      }
      .map(_.flatten.toList)

  def executeActionReturning[T](
      sql: String,
      prepare: Prepare = identityPrepare,
      extractor: Extractor[T],
      returningAction: ReturnAction
  )(info: ExecutionInfo, dc: Runner): Future[T] = {
    val (params, prepared) = prepare(Nil, ())
    logger.logQuery(sql, params)
    withClient(
      _.prepareAndQuery(expandAction(sql, returningAction), prepared)(
        extractor(_, ())
      )
    ).map(v => handleSingleResult(sql, v.toList))
  }

  private def expandAction(sql: String, returningAction: ReturnAction): String =
    returningAction match {
      // The Postgres dialect will create SQL that has a 'RETURNING' clause so we don't have to add one.
      case ReturnRecord => s"$sql"
      // The Postgres dialect will not actually use these below variants but in case we decide to plug
      // in some other dialect into this context...
      case ReturnColumns(columns) => s"$sql RETURNING ${columns.mkString(", ")}"
      case ReturnNothing          => s"$sql"
    }

  def executeBatchActionReturning[T](
      groups: List[BatchGroupReturning],
      extractor: Extractor[T]
  )(info: ExecutionInfo, dc: Runner): Future[List[T]] =
    Future
      .collect {
        groups.map { case BatchGroupReturning(sql, column, prepare) =>
          prepare
            .foldLeft(Future.value(List.newBuilder[T])) { case (acc, prepare) =>
              acc.flatMap { list =>
                executeActionReturning(sql, prepare, extractor, column)(
                  info,
                  dc
                ).map(
                  list += _
                )
              }
            }
            .map(_.result())
        }
      }
      .map(_.flatten.toList)

  def withClient[A](
      function: TwitterConnection.TwitterConnection => Future[A]
  ): Future[A] = twitterClientLocal() match {
    case Some(conn) => function(conn)
    case None =>
      twitterClientPool.usingFreshConnectionFromPool { conn =>
        twitterClientLocal.let(conn) { function(conn) }
      }
  }

  override private[getquill] def prepareParams(
      statement: String,
      prepare: Prepare
  ): Seq[String] =
    prepare(Nil, ())._2.map(prepareParam)

}
