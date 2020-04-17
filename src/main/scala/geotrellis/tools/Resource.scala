package geotrellis.tools

import java.io._
import java.net.{URI, URL}
import java.nio.charset.StandardCharsets
import org.apache.commons.io.IOUtils
import java.io.StringWriter
import org.apache.orc.DataMask.Standard

object Resource {
  def apply(name: String): String = {
    val stream: InputStream = getClass.getResourceAsStream(s"/$name")
    try {
      val writer = new StringWriter
      IOUtils.copy(stream, writer, StandardCharsets.UTF_8)
      writer.toString
    } finally {
      stream.close
    }
  }

  def url(name: String): URL = {
    getClass.getResource(s"/$name")
  }

  def uri(name: String): URI = {
    getClass.getResource(s"/$name").toURI
  }

  def path(name: String): String = {
    getClass.getResource(s"/$name").getFile
  }
}