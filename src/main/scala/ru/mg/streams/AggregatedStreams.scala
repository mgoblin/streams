package ru.mg.streams

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import ru.mg.aggregators.SeqAggregator
import ru.mg.domain.payment.Payment

object AggregatedStreams {

  private def groupOutgoings(aggregationWindow: SlidingEventTimeWindows): OutgoingGroupedStream =
    (dataStream: DataStream[Payment]) => dataStream
      .keyBy(_.fromPerson.name)
      .window(aggregationWindow)
      .aggregate(SeqAggregator[Payment])

  implicit class InputMixUtils(i: DataStream[Payment]) {
    def groupByOutgoings(aggregationWindow: SlidingEventTimeWindows): DataStream[OutgoingPaymentsGroup] =
      groupOutgoings(aggregationWindow)(i)
  }

}
