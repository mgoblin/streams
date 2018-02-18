package ru.mg.domain.payment

import java.time.LocalDateTime


case class Payment(fromPerson: Person, toPerson: Person, amount: Long, paymentDate: LocalDateTime) {
  require(amount > 0, s"Amount $amount does't meet requirement: Amount should be greater than zero")
  require(fromPerson != null, "Person from should not be null")
  require(toPerson != null, "Person to should not be null")
}
