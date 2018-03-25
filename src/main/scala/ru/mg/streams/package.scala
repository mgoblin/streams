package ru.mg

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import ru.mg.domain.payment.Payment

package object streams {
  type OutgoingPaymentsGroup = Seq[Payment]
  type OutgoingGroupedStream = DataStream[Payment] => DataStream[OutgoingPaymentsGroup]

  implicit class InputMixUtils(env: StreamExecutionEnvironment) {
    def fromPaymentsCsv(fileName: String): DataStream[Payment] = CsvFilePaymentsStream(env, fileName)
  }
}
