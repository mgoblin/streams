package ru.mg.streams

import java.time.{LocalDateTime, ZoneId}

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.scalatest.{FlatSpec, Matchers, PrivateMethodTester}
import ru.mg.domain.payment.{Payment, Person}
import ru.mg.utils.SinkCollector

class AggregatedStreamsSpec extends FlatSpec with Matchers with LazyLogging with PrivateMethodTester {

  "Aggregated streams" should "build flow for outgoing payments as a private method" in {

    val env = StreamExecutionEnvironment.createLocalEnvironment()
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val payments = Seq(
      Payment(fromPerson = Person("A"), toPerson = Person("B"), amount = 1000, LocalDateTime.now())
    )

    val paymentsStream = env
        .fromCollection(payments)
        .assignAscendingTimestamps(_.paymentDate.atZone(ZoneId.systemDefault()).toInstant.toEpochMilli)

    val flowBuilder = AggregatedStreams.groupOutgoings(
      SlidingEventTimeWindows.of(Time.seconds(2), Time.seconds(1))
    )

    val aggregateFlow = flowBuilder(paymentsStream)

    SinkCollector.clear()
    val collector = SinkCollector[OutgoingPaymentsGroup]

    aggregateFlow.addSink(g => collector.add(g))

    env.execute()

    SinkCollector.collector should contain theSameElementsAs List(payments, payments)

  }

  it should "mix groupByPersonOutgoings like flink operator to person payments stream" in {

    val env = StreamExecutionEnvironment.createLocalEnvironment()
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val payments = Seq(
      Payment(fromPerson = Person("A"), toPerson = Person("B"), amount = 1000, LocalDateTime.now())
    )

    SinkCollector.clear()
    val collector = SinkCollector[OutgoingPaymentsGroup]

    env
      .fromCollection(payments)
      .assignAscendingTimestamps(_.paymentDate.atZone(ZoneId.systemDefault()).toInstant.toEpochMilli)
      .groupByPersonOutgoings(SlidingEventTimeWindows.of(Time.seconds(2), Time.seconds(1)))
      .addSink(g => collector.add(g))

    env.execute()

    SinkCollector.collector should contain theSameElementsAs List(payments, payments)

  }

}
