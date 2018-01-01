package ru.mg

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import ru.mg.csv.CsvStreamBuilder

object Main extends LazyLogging {
  def main(args: Array[String]): Unit = {
    logger.info("Starting")

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val csvStreamBuilder = new CsvStreamBuilder("data/payments.csv")

    val stringInfo = createTypeInformation[String]
    val longInfo = createTypeInformation[Long]
    val input = csvStreamBuilder.build(env, Array(
      stringInfo,
      stringInfo,
      longInfo
    ))

    input.addSink(s => logger.info(s"$s"))

    env.execute("CSV reader")

    logger.info("Done")

  }
}
