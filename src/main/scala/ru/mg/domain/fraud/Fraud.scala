package ru.mg.domain.fraud

import ru.mg.domain.payment.{Payment, Person}

case class Fraud(person: Person, payments: Seq[Payment], description: String) {
  require(person != null, "Person should be not null")
  require(payments != null, "Payments should not be null")
  require(payments.nonEmpty, "Payments should not be empty")
  require(description != null && description.nonEmpty, "Description should not be null or empty")
}
