package com.covid19

import com.datastax.oss.driver.api.core.CqlSession
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader
import org.apache.log4j.PropertyConfigurator
import java.{util => ju}

object utils {

  def loadConfig(config: String): Config = {
    ConfigFactory.invalidateCaches()
    val root = ConfigFactory.load()
    val reference = root.getConfig("datastax-java-driver")
    val application = root.getConfig(config)
    application.withFallback(reference)
  }

  def createSession(arg: String): CqlSession = {
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

  def log_init(): Unit = {

    val f = getClass().getResource("/log4j.properties")
    PropertyConfigurator.configure(f.openStream())
  }
}
