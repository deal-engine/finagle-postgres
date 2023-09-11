package io.getquill.context.twitter

import com.twitter.util.Future

import java.io.Closeable
import com.typesafe.config.Config
import io.getquill.{JdbcContextConfig, NamingStrategy, PostgresDialect}
import io.getquill.context.jdbc.PostgresJdbcContextBase
import io.getquill.context.twitter.TwitterJdbcContext
import io.getquill.util.LoadConfig

import javax.sql.DataSource

class PostgresTwitterJdbcContext[+N <: NamingStrategy](
                                                      val naming: N,
                                                      val dataSource: DataSource with Closeable
                                                    ) extends TwitterJdbcContext[PostgresDialect, N](dataSource)
  with PostgresJdbcContextBase[PostgresDialect, N] {
  val idiom: PostgresDialect = PostgresDialect

  def this(naming: N, config: JdbcContextConfig) = this(naming, config.dataSource)
  def this(naming: N, config: Config) = this(naming, JdbcContextConfig(config))
  def this(naming: N, configPrefix: String) = this(naming, LoadConfig(configPrefix))


}