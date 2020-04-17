package geotrellis.jobs.hrpd

import geotrellis.proj4.util.UTM
import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.raster.summary.Statistics
import io.circe._
import io.circe.generic.semiauto._
import geotrellis.vector._


case class CountyResult(
  GEO_ID: String,
  tag: String,
  population: Long
) {
  def csvLine: String = List(
    GEO_ID,
    tag,
    population
  ).mkString(",")
}

object CountyResult {
  val csvHeader: String = "GEO_ID,TAG,POPULATION"
}