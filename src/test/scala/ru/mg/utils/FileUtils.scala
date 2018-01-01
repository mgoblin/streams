package ru.mg.utils

import java.io.{File, PrintWriter}

import scala.io.Source

object FileUtils {
  def createTempFile(resourceName: String): File = {
    val resourceStream = getClass.getResourceAsStream(s"/$resourceName")
    val fileLines = Source.fromInputStream(resourceStream).getLines().toList

    val tempFile = File.createTempFile(s"$resourceName", "")
    tempFile.deleteOnExit()

    val writer = new PrintWriter(tempFile)
    fileLines.foreach(l => writer.write(l))
    writer.close()

    tempFile
  }
}
