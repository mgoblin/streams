package ru.mg.domain.payment

import java.util.Date

case class Payment(from: Person, to: Person, amount: Long, date: Date) {
  require(amount > 0, s"Amount $amount does't meet requirement: Amount should be greater than zero")
  require(from != null, "Person from should not be null")
  require(to != null, "Person to should not be null")
}
