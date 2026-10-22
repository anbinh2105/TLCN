from transformation.silver.common import create_spark_session

spark = create_spark_session(
    app_name="CheckSparkMemory",
    master="local[2]",
)

try:
    runtime = spark._jvm.java.lang.Runtime.getRuntime()

    print("\n========== SPARK MEMORY CHECK ==========")
    print("Spark master:", spark.sparkContext.master)
    print(
        "Spark driver memory:",
        spark.sparkContext.getConf().get("spark.driver.memory")
    )
    print(
        "JVM max heap GB:",
        round(runtime.maxMemory() / (1024 ** 3), 2)
    )
    print("========================================\n")

finally:
    spark.stop()