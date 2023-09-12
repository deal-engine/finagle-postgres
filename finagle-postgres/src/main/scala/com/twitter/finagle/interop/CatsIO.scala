package com.twitter.finagle.interop

import cats.effect.IO
import cats.implicits.catsSyntaxTuple2Semigroupal
import fs2.Stream
import com.twitter.concurrent.AsyncStream
import com.twitter.util.{Future, Return, Throw}

object CatsIO {
  def fromTwitter[A](future: () => Future[A]): IO[A] = {
    IO.async_ { cb =>
      future().respond {
        case Throw(e)  => cb(Left(e))
        case Return(r) => cb(Right(r))
      }
    }
  }

  def fromTwitter[A](asyncStream: AsyncStream[A]): Stream[IO, A] =
    Stream.eval(CatsIO.fromTwitter(() => asyncStream.uncons)).flatMap {
      case Some((head, tail)) => Stream.emit(head) ++ fromTwitter(tail())
      case None               => Stream.empty
    }
}
