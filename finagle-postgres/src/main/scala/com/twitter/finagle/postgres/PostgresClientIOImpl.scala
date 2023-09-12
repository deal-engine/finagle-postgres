package com.twitter.finagle.postgres

import scala.concurrent.duration.{Duration => ScalaDuration}
import cats.effect.IO
import fs2.Stream
import com.twitter.cache.Refresh
import com.twitter.concurrent.AsyncStream
import com.twitter.conversions.DurationOps.*
import com.twitter.finagle.interop.CatsIO
import com.twitter.finagle.postgres.messages.*
import com.twitter.finagle.postgres.values.*
import com.twitter.finagle.{Service, ServiceFactory, Status}
import com.twitter.logging.Logger
import com.twitter.util.Future
import io.netty.buffer.ByteBuf

import java.nio.charset.{Charset, StandardCharsets}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import scala.language.{existentials, implicitConversions}
import scala.util.Random

/*
 * A Finagle client for communicating with Postgres.
 */

class PostgresClientIOImpl(
    factory: ServiceFactory[PgRequest, PgResponse],
    id: String,
    types: Option[Map[Int, PostgresClient.TypeSpecifier]] = None,
    receiveFunctions: PartialFunction[String, ValueDecoder[
      T
    ] forSome { type T }],
    binaryResults: Boolean = false,
    binaryParams: Boolean = false
) extends PostgresClientIO {
  private[this] val counter = new AtomicInteger(0)
  private[this] val logger = Logger(getClass.getName)
  private val resultFormats = if (binaryResults) Seq(1) else Seq(0)
  private val paramFormats = if (binaryParams) Seq(1) else Seq(0)

  def liftFuture: PostgresClientImpl = new PostgresClientImpl(
    factory,
    id,
    types,
    receiveFunctions,
    binaryResults,
    binaryParams
  )

  val charset = StandardCharsets.UTF_8

  private def retrieveTypeMap(): IO[Map[Int, PostgresClient.TypeSpecifier]] = {
    // get a mapping of OIDs to the name of the receive function for all types in the remote DB.
    // typreceive is the most reliable way to determine how a type should be decoded
    val customTypesQuery =
      """
      |SELECT DISTINCT
      |  CAST(t.typname AS text) AS type,
      |  CAST(t.oid AS integer) AS oid,
      |  CAST(t.typreceive AS text) AS typreceive,
      |  CAST(t.typelem AS integer) AS typelem
      |FROM           pg_type t
      |WHERE          CAST(t.typreceive AS text) <> '-'
    """.stripMargin

    val serviceF = CatsIO.fromTwitter(() => factory.apply)

    def extractTypes(
        response: PgResponse
    ): IO[Map[Int, PostgresClient.TypeSpecifier]] =
      response match {
        case SelectResult(fields, rows) =>
          val rowValues = ResultSet(
            fields,
            charset,
            rows,
            PostgresClient.defaultTypes,
            receiveFunctions
          ).rows
          CatsIO
            .fromTwitter { () =>
              rowValues
                .map { row =>
                  row.get[Int]("oid") -> PostgresClient.TypeSpecifier(
                    row.get[String]("typreceive"),
                    row.get[String]("type"),
                    row.get[Int]("typelem")
                  )
                }
                .toSeq()
            }
            .map(_.toMap)
      }

    val customTypesResult = for {
      service <- serviceF
      types <- (for {
        response <- CatsIO.fromTwitter(() =>
          service.apply(PgRequest(Query(customTypesQuery)))
        )
        types <- extractTypes(response)
      } yield types).guarantee(IO { service.close() })
    } yield types

    customTypesResult
  }

  private[postgres] val typeMap: IO[Map[Int, PostgresClient.TypeSpecifier]] =
    types.fold(retrieveTypeMap().memoize.flatten)(IO.pure)

  // The OIDs to be used when sending parameters
  private[postgres] val encodeOids =
    typeMap.map { tm =>
      tm.toIndexedSeq
        .map { case (oid, PostgresClient.TypeSpecifier(_, typeName, _)) =>
          typeName -> oid
        }
        .groupBy(_._1)
        .mapValues(_.map(_._2).min)
    }

  /*
   * Execute some actions inside of a transaction using a single connection
   */
  override def inTransaction[T](fn: PostgresClientIO => IO[T]): IO[T] =
    for {
      types <- typeMap
      service <- CatsIO.fromTwitter(() => factory())
      constFactory = ServiceFactory.const(service)
      id = Random.alphanumeric.take(28).mkString
      transactionalClient = new PostgresClientIOImpl(
        constFactory,
        id,
        Some(types),
        receiveFunctions,
        binaryResults,
        binaryParams
      )
      closeTransaction = () =>
        transactionalClient
          .close()
          .guarantee(
            CatsIO.fromTwitter(() =>
              constFactory.close().ensure(service.close())
            )
          )
      completeTransactionQuery = (sql: String) =>
        transactionalClient.query(sql).guarantee(closeTransaction())
      _ <- transactionalClient.query("BEGIN").onError(_ => closeTransaction())
      result <- fn(transactionalClient).handleErrorWith { case err =>
        for {
          _ <- completeTransactionQuery("ROLLBACK")
          _ <- IO.raiseError(err)
        } yield null.asInstanceOf[T]
      }
      _ <- completeTransactionQuery("COMMIT")
    } yield result

  /*
   * Issue an arbitrary SQL query and get the response.
   */
  override def query(sql: String): IO[QueryResponse] = sendQuery(sql) {
    case SelectResult(fields, rows) =>
      typeMap.map { types =>
        ResultSet(fields, charset, rows, types, receiveFunctions)
      }
    case CommandCompleteResponse(affected) => IO(OK(affected))
  }

  /*
   * Issue a single SELECT query and get the response.
   */
  override def fetch(sql: String): IO[SelectResult] = sendQuery(sql) {
    case rs: SelectResult => IO(rs)
  }

  /*
   * Execute an update command (e.g., INSERT, DELETE) and get the response.
   */
  override def executeUpdate(sql: String): IO[OK] = sendQuery(sql) {
    case CommandCompleteResponse(rows) => IO(OK(rows))
  }

  override def execute(sql: String): IO[OK] = executeUpdate(sql)

  /*
   * Run a single SELECT query and wrap the results with the provided function.
   */
  override def selectToStream[T](sql: String)(f: Row => T): Stream[IO, T] = {
    Stream.eval {
      for {
        types <- typeMap
        SelectResult(fields, rows) <- fetch(sql)
      } yield CatsIO
        .fromTwitter(
          ResultSet(fields, charset, rows, types, receiveFunctions).rows
        )
        .map(f)
    }.flatten
  }

  /*
   * Issue a single, prepared SELECT query and wrap the response rows with the provided function.
   */
  override def prepareAndQueryToStream[T](sql: String, params: Param[_]*)(
      f: Row => T
  ): Stream[IO, T] = {
    Stream
      .eval(typeMap.flatMap { _ =>
        for {
          service <- CatsIO.fromTwitter(() => factory())
          statement = new PreparedStatementImpl(sql, service)
          result <- statement.selectToStream(params: _*)(f)
        } yield result
      })
      .flatten
  }

  /*
   * Issue a single, prepared arbitrary query without an expected result set, and provide the affected row count
   */
  override def prepareAndExecute(
      sql: String,
      params: Param[_]*
  ): IO[Int] = {
    typeMap.flatMap { _ =>
      for {
        service <- CatsIO.fromTwitter(() => factory())
        statement = new PreparedStatementImpl(sql, service)
        OK(count) <- statement.exec(params: _*)
      } yield count
    }
  }

  /** Close the underlying connection pool and make this Client eternally down
    * @return
    */
  override def close(): IO[Unit] = {
    CatsIO.fromTwitter(() => factory.close())
  }

  /** The current availability [[Status]] of this client.
    */
  override def status: Status = factory.status

  /** Determines whether this client is available (can accept requests with a
    * reasonable likelihood of success).
    */
  override def isAvailable: Boolean = status == Status.Open

  private[this] def sendQuery[T](
      sql: String
  )(handler: PartialFunction[PgResponse, IO[T]]) = {
    send(PgRequest(Query(sql)))(handler)
  }

  private[this] def send[T](
      r: PgRequest,
      optionalService: Option[Service[PgRequest, PgResponse]] = None
  )(
      handler: PartialFunction[PgResponse, IO[T]]
  ) = {
    val service = optionalService.getOrElse(factory.toService)
    CatsIO
      .fromTwitter(() => service(r))
      .flatMap(handler orElse { case unexpected =>
        IO.raiseError(
          new IllegalStateException(s"Unexpected response $unexpected")
        )
      })
  }

  private[this] class PreparedStatementImpl(
      sql: String,
      service: Service[PgRequest, PgResponse]
  ) extends PreparedStatementIO {

    private[this] val name = s"fin-pg-$id-" + counter.incrementAndGet

    def closeService = service.close()

    private[this] def parse(params: Param[_]*): IO[Unit] = {
      val paramTypes = encodeOids.map { oidMap =>
        params.map { param =>
          oidMap.getOrElse(param.encoder.typeName, 0)
        }
      }
      paramTypes.flatMap { types =>
        val req = Parse(name, sql, types)
        send(PgRequest(req, flush = true), Some(service)) {
          case ParseCompletedResponse => IO.pure(())
        }
      }
    }

    private[this] def bind(params: Seq[ByteBuf]): IO[Unit] = {
      val req = Bind(
        portal = name,
        name = name,
        formats = paramFormats,
        params = params,
        resultFormats = resultFormats
      )

      send(PgRequest(req, flush = true), Some(service)) {
        case BindCompletedResponse => IO.pure(())
      }
    }

    private[this] def describe(): IO[Array[Field]] = {
      val req = PgRequest(Describe(portal = true, name = name), flush = true)
      send(req, Some(service)) { case RowDescriptions(fields) =>
        IO.pure(fields)
      }
    }

    private[this] def execute(
        maxRows: Int = 0
    ) = {
      val req = PgRequest(Execute(name, maxRows), flush = true)
      send(req, Some(service)) { case rep =>
        IO.pure(rep)
      }
    }

    private[this] def sync(
        optionalService: Option[Service[PgRequest, PgResponse]] = None
    ): IO[Unit] = send(PgRequest(Sync), optionalService) {
      case ReadyForQueryResponse => IO.pure(())
    }

    override def fire(params: Param[_]*): IO[QueryResponse] = {
      val paramBuffers = if (binaryParams) {
        params.map { p =>
          p.encodeBinary(StandardCharsets.UTF_8)
        }
      } else {
        params.map { p =>
          p.encodeText(StandardCharsets.UTF_8)
        }
      }

      val f = for {
        types <- typeMap
        pname <- parse(params: _*)
        _ <- bind(paramBuffers)
        fields <- describe()
        exec <- execute()
      } yield exec match {
        case CommandCompleteResponse(rows) => OK(rows)
        case Rows(rows) =>
          ResultSet(fields, charset, rows, types, receiveFunctions)
      }
      f.attempt
        .flatMap { result =>
          sync(Some(service)).flatMap { _ =>
            IO.fromEither(result)
          }
        }
        .guarantee(CatsIO.fromTwitter(() => service.close()))
    }
  }
}
