package io.getquill

import com.twitter.finagle.postgres.{Param, Row}
import io.getquill.context.Context

trait FinagleCtxBase[N <: NamingStrategy]
 extends Context[FinaglePostgresDialect, N] { this: Context[_, _] =>

  val naming: N

  type Session = Unit
   type Runner = Unit

   type PrepareRow = List[Param[_]]
   type ResultRow = Row

   type RunQueryResult[T] = List[T]
   type RunQuerySingleResult[T] = T
   type RunActionResult = Long
   type RunActionReturningResult[T] = T
   type RunBatchActionResult = List[Long]
   type RunBatchActionReturningResult[T] = List[T]

}
