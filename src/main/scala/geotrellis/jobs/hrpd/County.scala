package geotrellis.jobs.hrpd

import geotrellis.tools.Resource
import geotrellis.vector._
import geotrellis.vector.io.json.JsonFeatureCollection
import _root_.io.circe._
import _root_.io.circe.generic.semiauto._

object County {

  case class Id(GEO_ID: String)

  case class Props(
    GEO_ID: String,
    STATE: String,
    COUNTY: String,
    NAME: String,
    LSAD: String,
    CENSUSAREA: Double
  )

  def parse[T: Decoder]: Vector[Feature[MultiPolygon, T]] = {
    val collection = Resource("us_counties.geojson").parseGeoJson[JsonFeatureCollection]
    val ps = collection.getAllPolygonFeatures[T]
    val mps = collection.getAllMultiPolygonFeatures[T]
    ps.map(p => p.mapGeom(MultiPolygon(_))) ++ mps
  }

  implicit val idDecoder: Decoder[Id] = deriveDecoder[Id]
  implicit val idEncoder: Encoder[Id] = deriveEncoder[Id]

  implicit val propsDecoder: Decoder[Props] = deriveDecoder[Props]
  implicit val propsEncoder: Encoder[Props] = deriveEncoder[Props]
}
