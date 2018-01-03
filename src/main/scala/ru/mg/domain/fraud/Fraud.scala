package ru.mg.domain.fraud

import ru.mg.domain.payment.{Payment, Person}

case class Fraud(person: Person, payments: Seq[Payment], description: String)
