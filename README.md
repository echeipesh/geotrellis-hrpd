# Facebook HRPD Summary

This project performs polygonal summary over HRPD dataset for US County polygons and aggregates results in CSV.


## Data

US Counties GEOJSON is included in this project at `src/main/resources/us_counties.geojson`

Output is verified using following [gist](https://gist.github.com/echeipesh/78c324e1faac937bd9f96ff65626600d)

## Running on EMR

Update and verify EMR configuration values in `build.sbt` then:

```sh
sbt
sbt:geotrellis-hrpd> sparkCreateCluster
sbt:geotrellis-hrpd> sparkSubmitMain geotrellis.jobs.hrpd.PopSummaryApp --output s3://geotrellis-test/eac/county-hrdp-pop-v1 --partitions 2000
```

## Running Locally

This will be a bear to run locally.
For purpose of development and iteration you would need to trim the input set.

```sh
sbt
sbt:geotrellis-hrpd> test:runMain geotrellis.jobs.hrpd.PopSummaryApp --output file:/tmp/county.csv --partitions 2000
```