package ru.mg.detectors

import org.apache.flink.streaming.api.scala.DataStream
import ru.mg.domain.fraud.Fraud
import ru.mg.streams._
import org.apache.flink.api.scala._

object Detectors {

  def frequentOutgoings(threshold: Int): DataStream[OutgoingPayments] => DataStream[Fraud] = {
    dataStream: DataStream[OutgoingPayments] =>
      dataStream
        .filter(op => op.lengthCompare(threshold) > 0)
        .map(payments => Fraud(payments.head.from, payments, "frequent outgoing payments"))
  }
}
