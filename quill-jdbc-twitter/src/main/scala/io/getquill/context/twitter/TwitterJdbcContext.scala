package io.getquill.context.twitter

import cats.effect.kernel.Resource
import com.twitter.util.{Future, Local, Return, Throw}
import io.catbird.util.twitterFutureInstance

import java.io.Closeable
import java.sql.{Array as _, *}
import io.getquill.{NamingStrategy, ReturnAction}
import io.getquill.context.{Context, ContextVerbStream, ExecutionInfo, ProtoContext}
import io.getquill.context.jdbc.JdbcContextBase
import io.getquill.context.sql.idiom.SqlIdiom
import io.getquill.util.ContextLogger

import javax.sql.DataSource
import scala.util.Try

/**
 * Quill context that wraps all JDBC calls in `monix.eval.Task`.
 */
abstract class TwitterJdbcContext[+Dialect <: SqlIdiom, +Naming <: NamingStrategy](
                                                                                  dataSource: DataSource with Closeable,
                                                                                ) extends Context[Dialect, Naming]
  with ProtoContext[Dialect, Naming]
  with JdbcContextBase[Dialect, Naming] {

  override private[getquill] val logger = ContextLogger(classOf[TwitterJdbcContext[_, _]])

  override type Result[T] = Future[T]
  override type RunQueryResult[T] = List[T]
  override type RunQuerySingleResult[T] = T

  override type PrepareRow = PreparedStatement
  override type ResultRow = ResultSet
  override type RunActionResult = Long
  override type RunActionReturningResult[T] = T
  override type RunBatchActionResult = List[Long]
  override type RunBatchActionReturningResult[T] = List[T]

  override def wrap[T](t: => T): Future[T] = Future.value(t)

  override def push[A, B](result: Future[A])(f: A => B): Future[B] = result.map(f)

  override def seq[A](list: List[Future[A]]): Future[List[A]] = Future.traverseSequentially(list)(identity).map(_.toList)

  // Need explicit return-type annotations due to scala/bug#8356. Otherwise macro system will not understand Result[Long]=Task[Long] etc...
  override def executeAction(sql: String, prepare: Prepare = identityPrepare)(
    info: ExecutionInfo,
    dc: Runner
  ): Future[Long] =
    super.executeAction(sql, prepare)(info, dc)

  override def executeQuery[T](
                                sql: String,
                                prepare: Prepare = identityPrepare,
                                extractor: Extractor[T] = identityExtractor
                              )(info: ExecutionInfo, dc: Runner): Future[List[T]] =
    super.executeQuery(sql, prepare, extractor)(info, dc)

  override def executeQuerySingle[T](
                                      sql: String,
                                      prepare: Prepare = identityPrepare,
                                      extractor: Extractor[T] = identityExtractor
                                    )(info: ExecutionInfo, dc: Runner): Future[T] =
    super.executeQuerySingle(sql, prepare, extractor)(info, dc)

  override def executeActionReturning[O](
                                          sql: String,
                                          prepare: Prepare = identityPrepare,
                                          extractor: Extractor[O],
                                          returningBehavior: ReturnAction
                                        )(info: ExecutionInfo, dc: Runner): Future[O] =
    super.executeActionReturning(sql, prepare, extractor, returningBehavior)(info, dc)

  override def executeActionReturningMany[O](
                                              sql: String,
                                              prepare: Prepare = identityPrepare,
                                              extractor: Extractor[O],
                                              returningBehavior: ReturnAction
                                            )(info: ExecutionInfo, dc: Runner): Future[List[O]] =
    super.executeActionReturningMany(sql, prepare, extractor, returningBehavior)(info, dc)

  override def executeBatchAction(groups: List[BatchGroup])(info: ExecutionInfo, dc: Runner): Future[List[Long]] =
    super.executeBatchAction(groups)(info, dc)

  override def executeBatchActionReturning[T](groups: List[BatchGroupReturning], extractor: Extractor[T])(
    info: ExecutionInfo,
    dc: Runner
  ): Future[List[T]] =
    super.executeBatchActionReturning(groups, extractor)(info, dc)

  override def prepareQuery(sql: String, prepare: Prepare)(
    info: ExecutionInfo,
    dc: Runner
  ): Connection => Future[PreparedStatement] =
    super.prepareQuery(sql, prepare)(info, dc)

  override def prepareAction(sql: String, prepare: Prepare)(
    info: ExecutionInfo,
    dc: Runner
  ): Connection => Future[PreparedStatement] =
    super.prepareAction(sql, prepare)(info, dc)

  override def prepareBatchAction(
                                   groups: List[BatchGroup]
                                 )(info: ExecutionInfo, dc: Runner): Connection => Future[List[PreparedStatement]] =
    super.prepareBatchAction(groups)(info, dc)

  private val currentConnection: Local[Connection] = new Local[Connection]
  override def close(): Unit = dataSource.close()

  override protected def withConnection[T](f: Connection => Future[T]): Future[T] =
    for {
      maybeConnection <- Future(currentConnection())
      result <- maybeConnection match {
        case Some(connection) => f(connection)
        case None =>
            Future { dataSource.getConnection }.flatMap { conn =>
              f(conn).ensure(conn.close())
            }
      }
    } yield result

  def transaction[A](f: => Future[A]): Future[A] = for {
    maybeConnection <- Future(currentConnection())
    result <- maybeConnection match {
      case Some(_) => f
      case None =>
        Future {
          val c = dataSource.getConnection
          c.setAutoCommit(false)
          c
        }.flatMap { conn =>
          currentConnection.let(conn) {
            Future { f }.flatten.liftToTry.flatMap {
              case err@Throw(_) => Future { conn.rollback(); err }
              case ret@Return(_) => Future { conn.commit(); ret }
            }.lowerFromTry
          }
        }
    }
  } yield result

  // Override with sync implementation so will actually be able to do it.
  override def probe(sql: String): Try[_] = Try {
    val c = dataSource.getConnection
    try {
      c.createStatement().execute(sql)
    } finally {
      c.close()
    }
  }
}