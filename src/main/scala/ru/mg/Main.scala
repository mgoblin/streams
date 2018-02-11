package ru.mg

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import ru.mg.detectors.Detectors._
import ru.mg.domain.payment.Payment
import ru.mg.streams._
import ru.mg.streams.AggregatedStreams._

object Main extends LazyLogging {
  def main(args: Array[String]): Unit = {
    logger.info("Starting")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val paymentsStream: DataStream[Payment] = CsvFilePaymentsStream(env, "data/payments.csv")

    val tableEnv = TableEnvironment.getTableEnvironment(env)
    val paymentsTable = tableEnv.fromDataStream(paymentsStream, 'from, 'to, 'amount)
    tableEnv.registerTable("Payments", paymentsTable)


    paymentsStream
      .groupByOutgoings(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(15)))
      .findFrequentOutgoingsFraud(3)
      .addSink(s => logger.info(s"$s"))

    env.execute("CSV reader")

    logger.info("Done")

  }
}
