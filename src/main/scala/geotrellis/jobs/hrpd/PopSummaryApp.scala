package geotrellis.jobs.hrpd

import java.io.PrintWriter
import java.net.URI

import cats._
import cats.implicits._
import com.monovore.decline._
import geotrellis.layer._
import geotrellis.vector._
import geotrellis.raster._
import geotrellis.raster.summary.polygonal._
import geotrellis.raster.summary.polygonal.visitors._
import geotrellis.hrpd._
import cats.data.NonEmptyList
import geotrellis.raster.resample.NearestNeighbor
import geotrellis.raster.summary.types.SumValue
import geotrellis.raster.{GridExtent, RasterRegion}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import geotrellis.raster.summary.polygonal.visitors.SumVisitor.TileSumVisitor


object PopSummaryApp extends CommandApp(
  name = "Admin1 Population",
  header = "Summarize population within each Admin1 boundary",
  main = {
    val o = Opts.option[String](long = "output",
      help = "The path/uri of the summary JSON file")

    val p = Opts.option[Int]("partitions",
      help = "spark.default.parallelism").
      orNone

    (o, p).mapN { (output, numPartitions) =>

      System.setSecurityManager(null)
      val conf = new SparkConf()
        .setIfMissing("spark.master", "local[*]")
        .setAppName("HrpdPopSummary")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
        .set("spark.task.cpus", "1")
        .set("spark.default.parallelism", numPartitions.getOrElse(123).toString)
        .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35")
        .set("spark.network.timeout", "12000s")
        .set("spark.executor.heartbeatInterval", "600s")

      implicit val spark: SparkSession = SparkSession.builder.config(conf).enableHiveSupport.getOrCreate

      try {
        val regions = spark.sparkContext.broadcast(
          HRPD.rasters
        )

        val counties = spark.sparkContext.broadcast(
          County.parse[County.Id]
        )

        val job = new PopSummaryApp(regions, counties, numPartitions.getOrElse(123))
        val result: Array[((County.Id, String), PolygonalSummaryResult[SumValue])] =
          job.result.collect

        // Output

        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem.get(new URI(output), conf)
        val csv = new PrintWriter(fs.create(new Path(output + ".csv")))
        csv.println(CountyResult.csvHeader)
        try {
          for (((id, tag), sum) <- result) {

            sum.toOption match {
              case Some(sv) =>
                val result = CountyResult(id.GEO_ID, tag, sv.value.toLong)
                val line = result.csvLine
                println(line)
                csv.println(line)

              case None =>
                println(s"${id}, ${tag} didn't intersect any rasters")
            }
          }
        }
        finally {
          csv.close()
          fs.close()
        }

      } finally {
        spark.stop()
      }
    }
  }
)

class PopSummaryApp(
  regions: Broadcast[Vector[HrpdFile]],
  counties: Broadcast[Vector[Feature[MultiPolygon, County.Id]]],
  @transient val numPartitions: Int
)(implicit
  spark: SparkSession
) extends Serializable {
  // this is minimum chunk of work that may intersect multiple admin regions and raster regions
  val jobGrid = LayoutDefinition[Long](
    grid = GridExtent[Long](Extent(-180,-90,180,90), CellSize(2.77777777778E-4,2.77777777778E-4)),
    tileCols = 128*3, // evenly divides pixels into 3375 tiles
    tileRows = 64*3  // evenly divides pixels into 3375 tiles
  )

  // All the countries, value (CountryCode, RasterRegion)
  val regionRdd: RDD[(SpatialKey, Int)] = spark.sparkContext
    .parallelize(regions.value.indices, regions.value.length)
    .flatMap { fileIndex =>
      val source = regions.value(fileIndex).source.resampleToGrid(jobGrid, NearestNeighbor)
      val layoutTileSource: LayoutTileSource[SpatialKey] = LayoutTileSource.spatial(source, jobGrid)
      (layoutTileSource.keys.map { key =>
        key -> fileIndex
      })
    }.repartition(numPartitions)


  val result: RDD[((County.Id, String), PolygonalSummaryResult[SumValue])] =
    regionRdd
      .flatMap { case (key, fileIndex) =>
        val HrpdFile(source, tag) = regions.value.apply(fileIndex)
        val jobSource = source.resampleToGrid(jobGrid, NearestNeighbor)
        val layoutTileSource: LayoutTileSource[SpatialKey] = LayoutTileSource.spatial(jobSource, jobGrid)
        val region = layoutTileSource.rasterRegionForKey(key).get
        val bbox = region.extent.toPolygon()

        println(s"${key} -> ${source.name}")

        for {
          raster <- region.raster.map(_.mapTile(_.band(0))).toList
          feature <- counties.value.find(_.geom.intersects(bbox))
        } yield {
          (feature.data, tag) -> raster.polygonalSummary(feature.geom, new TileSumVisitor)
        }
      }
      .reduceByKey(_ combine _)
}
