
# Run from TLCN repository root.

if (-not (Test-Path ".env")) {
    throw ".env file not found"
}

docker run --rm `
  --name tlcn-spark-client `
  --network tlcn_default `
  --network-alias tlcn-spark-client `
  --user root `
  --env-file .env `
  -e SPARK_MODE=docker `
  -e MINIO_ENDPOINT=minio:9000 `
  -e HIVE_METASTORE_URI=thrift://hive-metastore:9083 `
  -v "${PWD}:/opt/project:ro" `
  --entrypoint /opt/spark/bin/spark-submit `
  tlcn-spark:4.1.1 `
  --master spark://spark-master:7077 `
  --deploy-mode client `
  --conf spark.driver.host=tlcn-spark-client `
  --conf spark.driver.bindAddress=0.0.0.0 `
  --conf spark.driver.port=37077 `
  --conf spark.blockManager.port=37078 `
  --conf spark.executor.memory=1g `
  --conf spark.executor.cores=1 `
  --conf spark.cores.max=1 `
  /opt/project/scripts/spark/check_weather_test_table.py

if ($LASTEXITCODE -ne 0) {
    throw "Weather Silver dry run failed"
}
