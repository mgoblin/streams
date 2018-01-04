package ru.mg

import org.apache.flink.streaming.api.scala.DataStream
import ru.mg.domain.payment.Payment

package object streams {
  type OutgoingPaymentsGroup = Seq[Payment]
  type OutgoingGroupedStream = DataStream[Payment] => DataStream[OutgoingPaymentsGroup]
}
