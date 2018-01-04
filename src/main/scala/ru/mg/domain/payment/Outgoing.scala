package ru.mg.domain.payment

case class Outgoing
(
  to: Person,
  amount: Long
)