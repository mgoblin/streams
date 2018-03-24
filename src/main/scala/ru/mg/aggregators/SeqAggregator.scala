package ru.mg.aggregators

import org.apache.flink.api.common.functions.AggregateFunction
import ru.mg.domain.payment.Payment

class SeqAggregator[T] extends AggregateFunction[T, Seq[T], Seq[T]] {

  override def add(value: T, accumulator: Seq[T]): Seq[T] = accumulator :+ value

  override def createAccumulator(): Seq[T] = Seq.empty

  override def getResult(accumulator: Seq[T]): Seq[T] = accumulator

  override def merge(a: Seq[T], b: Seq[T]): Seq[T] = a ++ b
}

object SeqAggregator {
  def apply[T]: SeqAggregator[T] = new SeqAggregator[T]
}
