package ru.mg.detectors

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}
import ru.mg.csv.PaymentsStream
import ru.mg.detectors.Detectors._
import ru.mg.domain.fraud.Fraud
import ru.mg.domain.payment.{Payment, Person}
import ru.mg.utils.{FileUtils, SinkCollector}

@RunWith(classOf[JUnitRunner])
class FrequentOutgoingSpec extends FlatSpec with Serializable with Matchers {
  "frequent outgoing detector" should "search payments from person in sliding window" in {
    val tempFile = FileUtils.createTempFile("payments.csv")

    val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism = 1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val input = PaymentsStream(env, tempFile.getAbsolutePath)

    SinkCollector.clear()
    val collector = SinkCollector[Fraud]

    val fraud = frequentOutgoings(1000, 500, 0)(input)

    fraud.addSink(f => collector.add(f))

    env.execute()

    SinkCollector.collector should have size 4

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    val date1 = sdf.parse("2018-01-01 12:18:00.000")
    val date2 = sdf.parse("2018-01-01 12:19:01.000")

    SinkCollector.collector should contain theSameElementsAs Seq(
      Fraud(Person("Mike"), Seq(Payment(Person("Mike"), Person("Elly"), 100, date1)), "dumb"),
      Fraud(Person("Mike"), Seq(Payment(Person("Mike"), Person("Elly"), 100, date1)), "dumb"),
      Fraud(Person("Jack"), Seq(Payment(Person("Jack"), Person("Mike"), 150, date2)), "dumb"),
      Fraud(Person("Jack"), Seq(Payment(Person("Jack"), Person("Mike"), 150, date2)), "dumb")
    )

  }
}
