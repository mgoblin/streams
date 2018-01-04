package ru.mg.aggregators

import org.apache.flink.api.common.functions.AggregateFunction
import ru.mg.domain.payment.{Outgoing, OutgoingAggregate, Payment, Person}

class OutgoingsAggregator extends AggregateFunction[Payment, Seq[Outgoing], OutgoingAggregate] {

  var fromPerson: Person = _

  override def createAccumulator(): Seq[Outgoing] = Seq.empty

  override def add(payment: Payment, accumulator: Seq[Outgoing]): Seq[Outgoing] = {
    synchronized(fromPerson = payment.from)
    accumulator :+ Outgoing(payment.to, payment.amount)
  }

  override def merge(a: Seq[Outgoing], b: Seq[Outgoing]): Seq[Outgoing] = a ++ b

  override def getResult(accumulator: Seq[Outgoing]): OutgoingAggregate = OutgoingAggregate(fromPerson, accumulator)
}
