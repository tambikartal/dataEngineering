object Main {

  import spark.implicits._

  import org.apache.spark.sql.types._

  /*load data from kafka*/
  val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "192.168.226.128:9092")
    .option("subscribe", "streaming-test")
    .option("startingOffsets", "earliest")
    .load()
  df.printSchema()

  /*select value and timestamp*/
  val selectDf = df.selectExpr("CAST(value AS STRING)","CAST(timestamp as TIMESTAMP)")

  /*#schema for value*/

  val dataSchema = StructType(
    List(
      StructField("filename", StringType, true),
      StructField("date", TimestampType, true),
      StructField("data1", StringType, true),
      StructField("data2", StringType, true),
      StructField("data3", StringType, true),
      StructField("data4", StringType, true),
      StructField("data5", StringType, true),
      StructField("data6", StringType, true),
      StructField("data7", StringType, true),
      StructField("data8", StringType, true),
      StructField("data9", StringType, true),
      StructField("data10", StringType, true),
      StructField("data11", StringType, true)
    )
  )


  /*value from json to dataframe with schema*/
  val df_from_json=stringDF.select(from_json($"value", dataSchema).alias("value"),$"timestamp")
  df_from_json.printSchema()


  /*select only timestamp and data1 column*/
  val df_not_count = df_from_json.selectExpr("timestamp","value.data1")
  df_not_count.printSchema()


  /*set time and coount data1*/
  val streamData = df_not_count.withWatermark("timestamp", "5 minutes")
    .groupBy(window($"timestamp", "5 minutes","1 minute"), $"data1")
    .count()
  streamData.printSchema()


  /*writeStream*/
  streamData.writeStream
    .outputMode("append")
    .format("json")
    .option("path", "/root/kafka_2.12-3.1.0/streamData")
    .option("header", true)
    .option("checkpointLocation", "/root/kafka_2.12-3.1.0/checkpointLocation/check")
    .option("numRows",4)
    .start()
    .awaitTermination()

  /*write stream for console

  #streamData.writeStream
  #.outputMode("append")
  #.format("console")
  #.option("header", true)
  #.option("numRows",4)
  #.start()
  #.awaitTermination()
  */
}