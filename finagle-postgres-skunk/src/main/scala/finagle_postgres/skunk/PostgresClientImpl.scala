package finagle_postgres.skunk

import java.nio.charset.{Charset, StandardCharsets}
import java.util.concurrent.atomic.AtomicBoolean
import cats.data.State
import cats.effect.std.Backpressure
import cats.effect.{IO, Resource}
import com.twitter.concurrent.AsyncStream
import com.twitter.finagle.Status
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.postgres.messages.SelectResult
import com.twitter.finagle.postgres.values.ValueDecoder
import com.twitter.finagle.postgres.{OK, Param, PostgresClient, QueryResponse, ResultSet, Row}
import com.twitter.io.StreamTermination.FullyRead
import com.twitter.util.{Future, Return, Throw, Try}
import skunk.*
import skunk.data.{Completion, Type}
import skunk.util.Origin

/*
 * A Skunk client for communicating with Postgres.
 * sessionPool must be pooled
 */

object PostgresClientImpl {
  private val sessionLocalKey = new Contexts.local.Key[Session[IO]]
}

class PostgresClientImpl(sessionPool: Resource[IO, Resource[IO, Session[IO]]], backpreassureConf: (Backpressure.Strategy, Int))
    extends PostgresClient { self =>
  protected val isActive: AtomicBoolean = new AtomicBoolean(true)
  protected val isTransaction: Boolean = false

  private var finalizerClosure: () => Future[Unit] = () => Future.Unit

  private final case class SessionPoolResource(pool: Resource[IO, Session[IO]], backpressure: Backpressure[IO])
  private val sessionPoolResource: Future[SessionPoolResource] =
    Util.runIO(
      for {
        sessionPool <- sessionPool.allocated
        _ = finalizerClosure = () => Util.runIO(sessionPool._2)
        backpressure <- Backpressure[IO](backpreassureConf._1, backpreassureConf._2)
      } yield SessionPoolResource(sessionPool._1, backpressure)
    )

  /*
  private def xuseSession[A](f: Session[IO] => Future[A]): Future[A] = {

    Contexts.local.get(PostgresClientImpl.sessionLocalKey) match {
      case Some(value) =>
        f(value)
      case None =>
        val s = sessionPoolResource.map(_.allocated).flatMap(Util.runIO)
        s.flatMap { case (current, finalizer) =>
          f(current).flatMap { result =>
            Util.runIO(finalizer).map(_ => result)
          }
        }
    }
  }*/

  private def useSessionIO[A](f: Session[IO] => IO[A]): Future[A] = {
    Contexts.local.get(PostgresClientImpl.sessionLocalKey) match {
      case Some(value) =>
        Util.runIO(f(value))
      case None =>
        Util.runIO(
          for {
            backpressure <- Util.toIO(() => sessionPoolResource.map(_.backpressure))
            sessionPool <- Util.toIO(() => sessionPoolResource.map(_.pool))
            maybeResult <- backpressure.metered { sessionPool.use(f) }
            result <- IO.fromOption(maybeResult)(new Exception("No more available spaces in pool."))
          } yield result
        )
    }
  }

  private def runTransaction[A](s: Session[IO])(fn: PostgresClient => Future[A]): IO[A] = {
    for {
      allocEffect <- s.transaction.allocated
      (trans, transFinalizer) = allocEffect
      result <- Util.toIO{ () =>
        Contexts.local.let(PostgresClientImpl.sessionLocalKey, s) {
          fn(new PostgresClientImpl(sessionPool, backpreassureConf) {
            override val isActive: AtomicBoolean = self.isActive
            override val isTransaction: Boolean = true
          })
        }
      }.onError { _ => transFinalizer}
      _ <- trans.commit
      _ <- transFinalizer
    } yield result
  }

  // private val sessionFuture: Future[Session[IO]] = alloc.map(_._1)
  // def as we dont want early execution
  private def finalizer(): Future[Unit] = finalizerClosure()

  override def charset: Charset = StandardCharsets.UTF_8

  override def inTransaction[T](fn: PostgresClient => Future[T]): Future[T] = {
    if (!isTransaction) {
      useSessionIO { s =>
        runTransaction(s)(fn)
      }
      // Allow nested transactions, probably a bad idea c:
      // Deal engine be like https://www.youtube.com/watch?v=5fbZTnZDvPA
    } else { fn(self) }
  }

  val decoder: Decoder[Row] = new Decoder[Row] {
    override def types: List[Type] = List.empty

    // INFO: offset is the current column, useful for returning an error with an specific
    // column. See Codec#simple
    // ss is a possible empty list (in that case we have 0 columns) with possible Null values
    // so None is NULL, and Some is a value without being decoded.
    // Converting to row should be trivial.
    // Note that I don't (currently) know how we handle NULL inside Row in the library, so either
    // need more investigation or we can try to just return as JVM's Null
    override def decode(
        offset: Int,
        ss: List[Option[String]]
    ): Either[Decoder.Error, Row] = Right(new Row {
      override def getOption[T](name: String)(implicit
          decoder: ValueDecoder[T]
      ): Option[T] =
        None

      override def getOption[T](index: Int)(implicit
          decoder: ValueDecoder[T]
      ): Option[T] =
        getTry(index).toOption

      override def get[T](name: String)(implicit decoder: ValueDecoder[T]): T =
        throw new UnsupportedOperationException()

      override def get[T](index: Int)(implicit decoder: ValueDecoder[T]): T = {
        decoder.decodeText("", ss(index).orNull).get()
      }

      override def getTry[T](name: String)(implicit
          decoder: ValueDecoder[T]
      ): Try[T] =
        Throw(new UnsupportedOperationException())

      override def getTry[T](index: Int)(implicit
          decoder: ValueDecoder[T]
      ): Try[T] = {
        decoder.decodeText("", ss(index).orNull)
      }

      override def getOrElse[T](name: String, default: => T)(implicit
          decoder: ValueDecoder[T]
      ): T =
        throw new UnsupportedOperationException()

      override def getOrElse[T](index: Int, default: => T)(implicit
          decoder: ValueDecoder[T]
      ): T =
        getTry(index).getOrElse(default)

      override def getAnyOption(name: String): Option[Any] =
        None

      override def getAnyOption(index: Int): Option[Any] =
        Try { ss(index) }.toOption.flatten
    })
  }

  override def query(sql: String): Future[QueryResponse] =
    useSessionIO { session =>
      for {
        xs <- session.execute(
          Query(
            sql = sql,
            origin = Origin.unknown,
            encoder = skunk.Void.codec,
            decoder = decoder,
            isDynamic = true
          )
        )
      } yield ResultSet(AsyncStream.fromSeq(xs))
    }

  override def fetch(sql: String): Future[SelectResult] = useSessionIO {
    session =>
      val query: Query[Void, Row] = Query(
        sql = sql,
        origin = Origin.unknown,
        encoder = skunk.Void.codec,
        decoder = decoder,
        isDynamic = true
      )
      session.execute(query).map { _ =>
        SelectResult.Empty // TODO maybe
      }
  }

  override def executeUpdate(sql: String): Future[OK] = execute(sql)

  override def execute(sql: String): Future[OK] = useSessionIO { session =>
    val command: Command[Void] =
      Command(sql = sql, origin = Origin.unknown, encoder = skunk.Void.codec)
    session.execute(command).map {
      case Completion.Insert(count) => OK(count)
      case Completion.Delete(count) => OK(count)
      case Completion.Select(count) => OK(count)
      case Completion.Update(count) => OK(count)
      case Completion.Copy(count)   => OK(count)
      case _                        => OK(0)
    }
  }

  override def selectToStream[T](sql: String)(f: Row => T): AsyncStream[T] = {
    val query: Query[Void, Row] = Query(
      sql = sql,
      origin = Origin.unknown,
      encoder = skunk.Void.codec,
      decoder = decoder,
      isDynamic = true
    )
    AsyncStream.fromFuture {
      useSessionIO { session =>
        fs2IO2Async(session.stream[Void, Row](query)(skunk.Void, 6).map(f))
      }
    }.flatten
  }

  val encoder: Encoder[Seq[Param[_]]] = new Encoder[Seq[Param[_]]] {
    override val sql: State[Int, String] = State { (n: Int) =>
      val len = types.length
      (n + len, (n until n + len).map(i => s"$$$i").mkString(", "))
    }

    override def types: List[Type] = List.empty

    override def encode(a: Seq[Param[_]]): List[Option[String]] = a.map { b =>
      def f[A](ba: Param[A]): Option[String] = {
        ba.encoder.encodeText(ba.value)
      }
      f(b)
    }.toList
  }

  def fs2IO2Async[A](stream: fs2.Stream[IO, A]): IO[AsyncStream[A]] = {
    val head = stream.head
    val tail = stream.tail
    head.compile.last.map {
      case Some(value) =>
        AsyncStream.mk(
          value,
          AsyncStream.fromFuture(Util.runIO(fs2IO2Async(tail))).flatten
        )
      case None => AsyncStream.empty
    }
  }

  override def prepareAndQueryToStream[T](sql: String, params: Param[_]*)(
      f: Row => T
  ): AsyncStream[T] = ???

  override def prepareAndQuery[T](sql: String, params: Param[_]*)(
      f: Row => T
  ): Future[Seq[T]] = useSessionIO { session =>
    val query: Query[Seq[Param[_]], Row] = Query(
      sql = sql,
      origin = Origin.unknown,
      encoder = encoder,
      decoder = decoder,
      isDynamic = true
    )

    for {
      pq <- session.prepare(query)
      r <- pq.stream(params, 6).map(f).compile.toList
    } yield r
  }

  override def prepareAndExecute(
      sql: String,
      params: Param[_]*
  ): Future[Int] = {
    useSessionIO { session =>
      val query: Command[Seq[Param[_]]] = Command(
        sql = sql,
        origin = Origin.unknown,
        encoder = encoder
      )

      for {
        pc <- session.prepare(query)
        r <- pc.execute(params)
        rr <- r match {
          case Completion.Insert(count) => IO.pure(count)
          case Completion.Delete(count) => IO.pure(count)
          case Completion.Update(count) => IO.pure(count)
          case tt =>
            IO.raiseError(
              new IllegalArgumentException(
                s"[$tt] shouldn't query in command code: $sql"
              )
            )
        }
      } yield rr
    }
  }

  /** Close the underlying connection pool and make this Client eternally down
    *
    * @return
    */
  override def close(): Future[Unit] = {
    isActive.set(false)
    finalizer()
  }

  /** The current availability [[Status]] of this client.
    */
  override def status: Status = if (isActive.get()) {
    Status.Open
  } else {
    Status.Closed
  }

  /** Determines whether this client is available (can accept requests with a
    * reasonable likelihood of success).
    */
  override def isAvailable: Boolean = isActive.get()
}
