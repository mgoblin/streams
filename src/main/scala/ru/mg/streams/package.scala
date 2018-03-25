package ru.mg

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import ru.mg.domain.payment.Payment
import ru.mg.streams.AggregatedStreams._

package object streams {
  type OutgoingPaymentsGroup = Seq[Payment]
  type OutgoingGroupedStream = DataStream[Payment] => DataStream[OutgoingPaymentsGroup]

  implicit class EnvMix(env: StreamExecutionEnvironment) {
    def fromPaymentsCsv(fileName: String): DataStream[Payment] = CsvFilePaymentsStream(env, fileName)
  }

  implicit class DataStreamMix(stream: DataStream[Payment]) {
    def groupByPersonOutgoings(aggregationWindow: SlidingEventTimeWindows): DataStream[OutgoingPaymentsGroup] =
      groupOutgoings(aggregationWindow)(stream)
  }
}
