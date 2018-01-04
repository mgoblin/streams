package ru.mg.aggregators

import org.apache.flink.api.common.functions.AggregateFunction
import ru.mg.domain.payment.Payment

class PaymentAggregator extends AggregateFunction[Payment, Seq[Payment], Seq[Payment]] {

  override def add(value: Payment, accumulator: Seq[Payment]): Seq[Payment] = accumulator :+ value

  override def createAccumulator(): Seq[Payment] = Seq.empty

  override def getResult(accumulator: Seq[Payment]): Seq[Payment] = accumulator

  override def merge(a: Seq[Payment], b: Seq[Payment]): Seq[Payment] = a ++ b
}
