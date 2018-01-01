package ru.mg.payment

import org.apache.flink.types.Row

object Payments {

  trait Converter[T] {
    def convert(row: Row): T
  }

  implicit object PaymentConverter extends Converter[Payment] {
    override def convert(row: Row): Payment = {
      require(row.getArity == 3, s"Row arity ${row.getArity} should be 3")
      require(row.getField(0).isInstanceOf[String], "Row field(0) should be String")
      require(row.getField(1).isInstanceOf[String], "Row field(1) should be String")
      require(row.getField(2).isInstanceOf[Long], "Row field(2) should be Long")

      Payment(
        Person(row.getField(0).asInstanceOf[String]),
        Person(row.getField(1).asInstanceOf[String]),
        row.getField(2).asInstanceOf[Long]
      )
    }
  }

  implicit class ConverterUtil[P](p: P) {
    def convertFrom[P](row: Row)(implicit evidence: Converter[P]): P = implicitly[Converter[P]].convert(row)
  }

}
