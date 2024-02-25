from sedona.spark import SedonaContext


def fetch_for_name(name):
    config = (SedonaContext.builder()
              .master("local[*]")
              .config("spark.driver.bindAddress", "127.0.0.1")
              .config("spark.network.timeout", "6001s")
              .config("spark.executor.heartbeatInterval", "6000s")
              .appName(name)
              .config("spark.jars.packages",
                      "org.apache.sedona:sedona-spark-3.0_2.12:1.5.1,org.datasyslab:geotools-wrapper:1.5.1-28.2")
              .config("spark.jars.repositories", "https://artifacts.unidata.ucar.edu/repository/unidata-all")
              .getOrCreate())
    return SedonaContext.create(config)
