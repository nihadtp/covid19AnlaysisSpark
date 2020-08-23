package com.covid19

import org.joda.time.DateTime
import com.datastax.oss.driver.api.core.config.DriverConfigLoader
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.oss.driver.api.core.CqlSession
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader
import com.datastax.oss.driver.api.core.config.DefaultDriverOption

object utils {

  def loadConfig(config: String): Config = {
    ConfigFactory.invalidateCaches()
    val root = ConfigFactory.load()
    val reference = root.getConfig("datastax-java-driver")
    val application = root.getConfig(config)
    application.withFallback(reference)
  }

  def createSession(arg: String) = {
    arg match {
      case "local" => {
        val loader = new DefaultDriverConfigLoader(() =>
          loadConfig("local_cassandra")
        )
        CqlSession.builder().withConfigLoader(loader).build()
      }

      case "aws" => {

        val loader = new DefaultDriverConfigLoader(() =>
          loadConfig("amazon_cassandra")
        )
        CqlSession.builder().withConfigLoader(loader).build()
      }

    }
  }
}
