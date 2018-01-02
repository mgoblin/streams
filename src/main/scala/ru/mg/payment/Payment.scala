package ru.mg.payment

import java.util.Date

case class Payment(from: Person, to: Person, amount: Long, date: Date) {
  require(amount > 0, s"Amount $amount does't meet requirement: Amount should be greater than zero")
}
