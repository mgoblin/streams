package ru.mg

import java.util.Date

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import ru.mg.csv.PaymentsStream
import ru.mg.detectors.Detectors._
import ru.mg.domain.payment.{Payment, Person}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic

object Main extends LazyLogging {
  def main(args: Array[String]): Unit = {
    logger.info("Starting")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val now: Date = new Date(System.currentTimeMillis())
    val socketStream = env.socketTextStream("127.0.0.1", 9999).map(s => Payment(Person("x"), Person("y"), 0, now))

    val input = PaymentsStream(env, "data/payments.csv")//.union(socketStream)

    frequentOutgoings(input)
      .addSink(s => logger.info(s"$s"))

    env.execute("CSV reader")

    logger.info("Done")

  }
}
