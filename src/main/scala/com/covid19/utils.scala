package com.covid19

import org.joda.time.DateTime
import com.datastax.oss.driver.api.core.CqlSession
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader
import com.datastax.oss.driver.api.core.config.DefaultDriverOption
import _root_.java.io.InputStream
import java.io.FileInputStream
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
    // val inputForLog =
    //   (new FileInputStream("src/main/resources/log4j.properties"))
    //     .asInstanceOf[InputStream]

    val f = getClass().getResource("/log4j.properties")
    PropertyConfigurator.configure(f.openStream())
  }
}
