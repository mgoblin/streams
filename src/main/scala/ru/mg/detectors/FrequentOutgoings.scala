package ru.mg.detectors

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.time.Time
import ru.mg.domain.fraud.{Fraud, FraudDetector}
import ru.mg.domain.payment.{Payment, Person}
import org.apache.flink.api.scala._

class FrequentOutgoings extends FraudDetector {

  private val aggregatePayments = new AggregateFunction[Payment, Fraud, Fraud] {

    override def createAccumulator(): Fraud = Fraud(Person(""), Seq.empty, "dumb")

    override def add(value: Payment, accumulator: Fraud): Fraud =
      accumulator
        .copy(
          person = value.from,
          payments = accumulator.payments :+ value
        )

    override def getResult(accumulator: Fraud): Fraud = accumulator

    override def merge(a: Fraud, b: Fraud): Fraud = a.copy(payments = a.payments ++ b.payments )
  }

  override def analyze(dataStream: DataStream[Payment]): DataStream[Fraud] =
    dataStream
      .keyBy(_.from.name)
      .timeWindow(Time.seconds(10), Time.seconds(3))
      .aggregate(aggregatePayments)
      .filter(_.payments.lengthCompare(3) > 0)
}
