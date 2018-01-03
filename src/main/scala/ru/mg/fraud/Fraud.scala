package ru.mg.fraud

import ru.mg.payment.{Payment, Person}

case class Fraud(person: Person, payments: Seq[Payment], description: String)
