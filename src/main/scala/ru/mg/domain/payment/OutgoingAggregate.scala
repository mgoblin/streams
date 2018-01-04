package ru.mg.domain.payment

case class OutgoingAggregate
(
  person: Person,
  outgoings: Seq[Outgoing] = Seq.empty
)
