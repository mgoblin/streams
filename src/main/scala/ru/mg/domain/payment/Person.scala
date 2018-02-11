package ru.mg.domain.payment

case class Person(name: String) {
  require(name != null && name.nonEmpty, "Name should be not null and not empty")
}
