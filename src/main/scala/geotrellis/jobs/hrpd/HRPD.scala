package geotrellis.hrpd

import geotrellis.raster._

import software.amazon.awssdk.services.s3.model._
import geotrellis.store.s3.S3ClientProducer
import scala.collection.JavaConverters._
import java.net.URI

case class HrpdFile(source: RasterSource, tag: String)

object HRPD {

  // ex: s3://azavea-humdata/facebook/hrpd/USA_lat_54_lon_-141_men.tif -> men
  val tagRX = """.*_lat_\d+_lon_-?\d+_(.*)\.tif""".r

  def listBucket(bucket: String, prefix: String): Iterator[String] = {
    val client = S3ClientProducer.get()
    var req = ListObjectsV2Request.builder.bucket(bucket).prefix(prefix).build
    val res = client.listObjectsV2(req)
    val iter = res.contents.iterator.asScala
    iter.map { o =>
      s"s3://$bucket/${o.key}"
    }
  }

  def rasters: Vector[HrpdFile] = {
    listBucket("azavea-humdata", "facebook/hrpd/")
      .map{ uri =>
        val tag = tagRX.findFirstMatchIn(uri.toString).get.group(1)
        val source = RasterSource(uri)
        HrpdFile(source, tag)
      }
      .toVector
  }
}