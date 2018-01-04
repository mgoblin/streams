package ru.mg.streams

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import ru.mg.aggregators.PaymentAggregator
import ru.mg.domain.payment.Payment

object AggregatedStreams {

  private val aggregatePayments = new PaymentAggregator

  def groupOutgoings(aggregationWindow: SlidingEventTimeWindows): OutgoingGroupedStream =
    (dataStream: DataStream[Payment]) => dataStream
      .keyBy(_.from.name)
      .window(aggregationWindow)
      .aggregate(aggregatePayments)

}
