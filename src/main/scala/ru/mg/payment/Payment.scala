package ru.mg.payment

case class Payment(from: Person, to: Person, amount: Long) {
  require(amount > 0, s"Amount $amount does't meet requirement: Amount should be greater than zero")
}
