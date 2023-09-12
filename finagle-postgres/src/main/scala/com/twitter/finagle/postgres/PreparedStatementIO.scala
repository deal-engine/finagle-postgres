package com.twitter.finagle.postgres

import cats.effect.IO
import fs2.Stream
import com.twitter.concurrent.AsyncStream
import com.twitter.finagle.interop.CatsIO
import com.twitter.finagle.postgres.codec.Errors

/*
 * A query that supports parameter substitution. Can help prevent SQL injection attacks.
 */
trait PreparedStatementIO {
  def fire(params: Param[_]*): IO[QueryResponse]

  def exec(params: Param[_]*): IO[OK] = fire(params: _*) flatMap {
    case ok @ OK(_) => IO.pure(ok)
    case ResultSet(_) =>
      IO.raiseError(Errors.client("Update query expected"))
  }

  def selectToStream[T](
      params: Param[_]*
  )(f: Row => T): IO[Stream[IO, T]] = fire(params: _*) map {
    case ResultSet(rows) => CatsIO.fromTwitter(rows).map(f)
    case OK(_)           => Stream.empty
  }

  def select[T](params: Param[_]*)(f: Row => T): IO[Seq[T]] = {
    selectToStream(params: _*)(f).flatMap(_.compile.toList)
  }

  def selectFirst[T](params: Param[_]*)(f: Row => T): IO[Option[T]] =
    select[T](params: _*)(f) flatMap { rows => IO.pure(rows.headOption) }
}
