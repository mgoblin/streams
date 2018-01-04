package ru.mg.detectors

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import ru.mg.aggregators.PaymentAggregator
import ru.mg.domain.fraud.{Fraud, FraudDetector}
import ru.mg.domain.payment.Payment

class FrequentOutgoings(aggregationWindow: SlidingEventTimeWindows, val threshold: Int) extends FraudDetector with Serializable {

  private val aggregatePayments = new PaymentAggregator

  override def name: String = "frequent outgoing payments"

  override def analyze(dataStream: DataStream[Payment]): DataStream[Fraud] =
    dataStream
      .keyBy(_.from.name)
      .window(aggregationWindow)
      .aggregate(aggregatePayments)
      .filter(_.lengthCompare(threshold) > 0)
      .map(payments => {
        Fraud(payments.head.from, payments, name)
      })
}
