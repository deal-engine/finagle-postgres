package com.twitter.finagle.postgres

import cats.effect.IO
import fs2.Stream

import java.nio.charset.Charset
import com.twitter.concurrent.AsyncStream
import com.twitter.finagle.Status
import com.twitter.finagle.postgres.messages.SelectResult
import com.twitter.finagle.postgres.values.Types

trait PostgresClientIO {

  def charset: Charset

  /*
   * Execute some actions inside of a transaction using a single connection
   */
  def inTransaction[T](fn: PostgresClientIO => IO[T]): IO[T]

  /*
   * Issue an arbitrary SQL query and get the response.
   */
  def query(
      sql: String
  ): IO[QueryResponse]

  /*
   * Issue a single SELECT query and get the response.
   */
  def fetch(
      sql: String
  ): IO[SelectResult]

  /*
   * Execute an update command (e.g., INSERT, DELETE) and get the response.
   */
  def executeUpdate(
      sql: String
  ): IO[OK]

  def execute(sql: String): IO[OK]

  /*
   * Run a single SELECT query and wrap the results with the provided function.
   */
  def select[T](sql: String)(f: Row => T): IO[Seq[T]] =
    selectToStream(sql)(f).compile.toList
  def selectToStream[T](sql: String)(f: Row => T): Stream[IO, T]

  /*
   * Issue a single, prepared SELECT query and wrap the response rows with the provided function.
   */
  def prepareAndQuery[T](sql: String, params: Param[_]*)(
      f: Row => T
  ): IO[Seq[T]] =
    prepareAndQueryToStream(sql, params: _*)(f).compile.toList
  def prepareAndQueryToStream[T](sql: String, params: Param[_]*)(
      f: Row => T
  ): Stream[IO, T]

  /*
   * Issue a single, prepared arbitrary query without an expected result set, and provide the affected row count
   */
  def prepareAndExecute(
      sql: String,
      params: Param[_]*
  ): IO[Int]

  /** Close the underlying connection pool and make this Client eternally down
    *
    * @return
    */
  def close(): IO[Unit]

  /** The current availability [[Status]] of this client.
    */
  def status: Status

  /** Determines whether this client is available (can accept requests with a
    * reasonable likelihood of success).
    */
  def isAvailable: Boolean
}
