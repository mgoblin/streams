package ru.mg.aggregators

import java.time.LocalDateTime

import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{FlatSpec, Matchers}
import ru.mg.domain.payment.{Payment, Person}

class PaymentAggregatorSpec extends FlatSpec with Matchers with LazyLogging {

  val aggregator = new PaymentAggregator

  val payment = Payment(
    fromPerson = Person("A"),
    toPerson = Person("B"),
    amount = 1000,
    paymentDate = LocalDateTime.now()
  )

  "Payment aggregator" should "add values to accumulator" in {
    aggregator.add(payment, Seq()) should contain theSameElementsAs Seq(payment)
    aggregator.add(payment, Seq(payment)) should contain theSameElementsAs Seq(payment, payment)

  }

  it should "create empty accumulator" in {
    aggregator.createAccumulator() should be(empty)
  }

  it should "get accumulator as result" in {
    aggregator.getResult(Seq()) should contain theSameElementsAs Seq()
    aggregator.getResult(Seq(payment)) should contain theSameElementsAs Seq(payment)
  }

  it should "merge accumulators" in {
    aggregator.merge(Seq(payment), Seq(payment)) should contain theSameElementsAs Seq(payment, payment)
  }
}
