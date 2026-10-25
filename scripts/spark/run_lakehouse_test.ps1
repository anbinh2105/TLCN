
# Run from the TLCN repository root.

$envFile = Join-Path (Get-Location) ".env"

if (-not (Test-Path $envFile)) {
    throw ".env file not found"
}

# Read .env without printing credentials.
$envValues = @{}

Get-Content $envFile | ForEach-Object {
    $line = $_.Trim()

    if (
        $line -and
        -not $line.StartsWith("#") -and
        $line.Contains("=")
    ) {
        $parts = $line.Split("=", 2)
        $key = $parts[0].Trim()
        $value = $parts[1].Trim().Trim('"').Trim("'")

        $envValues[$key] = $value
    }
}

$accessKey = $envValues["MINIO_ACCESS_KEY"]
$secretKey = $envValues["MINIO_SECRET_KEY"]

if (-not $accessKey -or -not $secretKey) {
    throw "Missing MinIO credentials in .env"
}

# Pass credentials into Docker container.
$env:MINIO_ACCESS_KEY = $accessKey
$env:MINIO_SECRET_KEY = $secretKey

try {
    docker run --rm `
      --name tlcn-spark-client `
      --network tlcn_default `
      --network-alias tlcn-spark-client `
      --user root `
      -e MINIO_ACCESS_KEY `
      -e MINIO_SECRET_KEY `
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
      --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions `
      --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog `
      --conf spark.sql.catalog.lakehouse.type=hive `
      --conf spark.sql.catalog.lakehouse.uri=thrift://hive-metastore:9083 `
      --conf spark.sql.catalog.lakehouse.warehouse=s3a://flood-lakehouse/warehouse `
      --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 `
      --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider `
      --conf spark.hadoop.fs.s3a.path.style.access=true `
      --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false `
      --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem `
      /opt/project/scripts/spark/lakehouse_connection_test.py

    if ($LASTEXITCODE -ne 0) {
        throw "Spark job failed with exit code $LASTEXITCODE"
    }
}
finally {
    Remove-Item Env:MINIO_ACCESS_KEY -ErrorAction SilentlyContinue
    Remove-Item Env:MINIO_SECRET_KEY -ErrorAction SilentlyContinue
}
