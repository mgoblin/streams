package ru.mg

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import ru.mg.detectors.Detectors._
import ru.mg.streams.CsvFilePaymentsStream

object Main extends LazyLogging {
  def main(args: Array[String]): Unit = {
    logger.info("Starting")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val input = CsvFilePaymentsStream(env, "data/payments.csv")

    frequentOutgoings(input)
      .addSink(s => logger.info(s"$s"))

    env.execute("CSV reader")

    logger.info("Done")

  }
}
