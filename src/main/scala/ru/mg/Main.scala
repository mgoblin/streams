package ru.mg

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import ru.mg.detectors.Detectors._
import ru.mg.streams._
import ru.mg.streams.AggregatedStreams._

object Main extends LazyLogging {
  def main(args: Array[String]): Unit = {
    logger.info("Starting")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val input = CsvFilePaymentsStream(env, "data/payments.csv")
    val groupByOutgoings = groupOutgoings(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(15)))

    frequentOutgoings(3)(groupByOutgoings(input))
      .addSink(s => logger.info(s"$s"))

    env.execute("CSV reader")

    logger.info("Done")

  }
}
