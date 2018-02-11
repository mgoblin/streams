package ru.mg.domain.payment

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.types.Row

object Payments extends LazyLogging {

  trait Converter[T] {
    def convert(row: Row): T
  }

  implicit object PaymentConverter extends Converter[Payment] {

    private[this] val dateTimeFormat = "yyyy-MM-dd HH:mm:ss.SSS"
    private[this] val formatter = DateTimeFormatter.ofPattern(dateTimeFormat)

    override def convert(row: Row): Payment = {
      logger.debug(s"Converting $row to payment")

      require(row.getArity == 4, s"Row arity ${row.getArity} should be 4")
      require(row.getField(0).isInstanceOf[String], "Row field(0) should be String")
      require(row.getField(1).isInstanceOf[String], "Row field(1) should be String")
      require(row.getField(2).isInstanceOf[Number], "Row field(2) should be Long number")
      require(row.getField(3).isInstanceOf[String], "Row field(3) should be String")

      val payment = Payment(
        Person(row.getField(0).asInstanceOf[String]),
        Person(row.getField(1).asInstanceOf[String]),
        row.getField(2).asInstanceOf[Long],
        LocalDateTime.parse(row.getField(3).asInstanceOf[String], formatter)
      )

      logger.debug(s"Row $row converted to payment $payment")
      payment
    }
  }

  implicit class ConverterUtil(row: Row) {
    def as[P](implicit evidence: Converter[P]): P = implicitly[Converter[P]].convert(row)
  }

}
