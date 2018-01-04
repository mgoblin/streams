package ru.mg.utils

import java.io.Serializable


object SinkCollector extends Serializable {
  def apply[T]: SinkCollector[T] = new SinkCollector[T]()
  val collector: java.util.List[Any] = new java.util.concurrent.CopyOnWriteArrayList
  def clear(): Unit = collector.clear()
}

class SinkCollector[T] extends Serializable {
  import SinkCollector._

  def add(v: T): Unit = collector.add(v)
}
