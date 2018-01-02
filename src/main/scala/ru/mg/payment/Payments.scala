package ru.mg.payment

import java.text.SimpleDateFormat

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.types.Row

object Payments extends LazyLogging {

  trait Converter[T] {
    def convert(row: Row): T
  }

  implicit object PaymentConverter extends Converter[Payment] {

    val dateTimeFormat = "yyyy-MM-dd HH:mm:ss.SSS"
    val formatter = new SimpleDateFormat(dateTimeFormat)

    override def convert(row: Row): Payment = {
      logger.debug(s"Converting $row to payment")

      require(row.getArity == 4, s"Row arity ${row.getArity} should be 4")
      require(row.getField(0).isInstanceOf[String], "Row field(0) should be String")
      require(row.getField(1).isInstanceOf[String], "Row field(1) should be String")
      require(row.getField(2).isInstanceOf[Long], "Row field(2) should be Long")
      require(row.getField(3).isInstanceOf[String], "Row field(3) should be String")

      val payment = Payment(
        Person(row.getField(0).asInstanceOf[String]),
        Person(row.getField(1).asInstanceOf[String]),
        row.getField(2).asInstanceOf[Long],
        formatter.parse(row.getField(3).asInstanceOf[String])
      )

      logger.debug(s"Row $row converted to payment $payment")
      payment
    }
  }

  implicit class ConverterUtil[P](p: P) {
    def convertFrom[P](row: Row)(implicit evidence: Converter[P]): P = implicitly[Converter[P]].convert(row)
  }

}
