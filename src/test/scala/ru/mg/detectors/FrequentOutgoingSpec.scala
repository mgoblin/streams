package ru.mg.detectors

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}
import ru.mg.detectors.Detectors._
import ru.mg.domain.fraud.Fraud
import ru.mg.domain.payment.{Payment, Person}
import ru.mg.streams.AggregatedStreams._
import ru.mg.streams.CsvFilePaymentsStream
import ru.mg.utils.{FileUtils, SinkCollector}


@RunWith(classOf[JUnitRunner])
class FrequentOutgoingSpec extends FlatSpec with Serializable with Matchers {
  "frequent outgoing detector" should "search frequent payments from person in sliding window" in {
    val tempFile = FileUtils.createTempFile("payments.csv")

    val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism = 1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val input = CsvFilePaymentsStream(env, tempFile.getAbsolutePath)

    SinkCollector.clear()
    val collector = SinkCollector[Fraud]

    input
      .groupByOutgoings(SlidingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(500)))
      .findFrequentOutgoingsFraud(0)
      .addSink(f => collector.add(f))

    env.execute()

    SinkCollector.collector should have size 4

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    val date1 = sdf.parse("2018-01-01 12:18:00.000")
    val date2 = sdf.parse("2018-01-01 12:19:01.000")

    SinkCollector.collector should contain theSameElementsAs Seq(
      Fraud(Person("Mike"), Seq(Payment(Person("Mike"), Person("Elly"), 100, date1)), "frequent outgoing payments"),
      Fraud(Person("Mike"), Seq(Payment(Person("Mike"), Person("Elly"), 100, date1)), "frequent outgoing payments"),
      Fraud(Person("Jack"), Seq(Payment(Person("Jack"), Person("Mike"), 150, date2)), "frequent outgoing payments"),
      Fraud(Person("Jack"), Seq(Payment(Person("Jack"), Person("Mike"), 150, date2)), "frequent outgoing payments")
    )

  }

  it should "not search fraud if payments count less than threshold" in {
    val tempFile = FileUtils.createTempFile("payments.csv")

    val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism = 1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    SinkCollector.clear()
    val collector = SinkCollector[Fraud]

    val input = CsvFilePaymentsStream(env, tempFile.getAbsolutePath)
    input
      .groupByOutgoings(SlidingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(500)))
      .findFrequentOutgoingsFraud(1)
      .addSink(f => collector.add(f))

    env.execute()

    SinkCollector.collector should have size 0

  }

  it should "search fraud more than threshold in a sliding window" in {
    val tempFile = FileUtils.createTempFile("freqPayments.csv")

    val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism = 1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    SinkCollector.clear()
    val collector = SinkCollector[Fraud]

    val input = CsvFilePaymentsStream(env, tempFile.getAbsolutePath)
    input
      .groupByOutgoings(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(8)))
      .findFrequentOutgoingsFraud(1)
      .addSink(f => collector.add(f))

    env.execute()

    SinkCollector.collector should have size 1

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    val date1 = sdf.parse("2018-01-01 12:19:00.000")
    val date2 = sdf.parse("2018-01-01 12:19:03.000")

    SinkCollector.collector should contain theSameElementsAs Seq(
      Fraud(
        Person("Mike"),
        Seq(
          Payment(Person("Mike"), Person("Elly"), 100, date1),
          Payment(Person("Mike"), Person("Alice"), 120, date2)
        ),
        "frequent outgoing payments"
      )
    )

  }
}
