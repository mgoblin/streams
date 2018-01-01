package ru.mg

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

object Main extends LazyLogging {
  def main(args: Array[String]): Unit = {
    logger.info("Starting")

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val socketStream = env.fromElements(1, 2, 3)

    socketStream
      .addSink(s => logger.info(s"$s"))

    env.execute("Socket reader")

    logger.info("Done")

  }
}
