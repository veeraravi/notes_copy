import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import org.apache.spark.sql.avro.functions.from_avro

object KafkaStructuredStreamingWithSecurity {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("KafkaStructuredStreamingWithSchemaRegistryAndSecurity")
      .master("local[*]") // remove for production
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    // -------------------------
    // Kafka + Schema Registry configs
    // -------------------------
    val kafkaBrokers       = "broker1:9093,broker2:9093"
    val kafkaTopic         = "secure-topic"
    val schemaRegistryUrl  = "https://schemaregistry:8081"

    // -------------------------
    // Read stream with ALL possible Kafka options
    // -------------------------
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)

      // Topic subscription (choose one)
      .option("subscribe", kafkaTopic)                        // single / multiple topics
      //.option("assign", """{"secure-topic":[0,1]}""")        // specific partitions
      //.option("subscribePattern", "secure.*")                // regex subscription

      // Offset handling
      .option("startingOffsets", "earliest")                  // earliest/latest or per-partition JSON
      //.option("startingOffsets", """{"secure-topic":{"0":5}}""")
      .option("failOnDataLoss", "false")                      // ignore if data lost
      .option("maxOffsetsPerTrigger", "5000")                 // limit records per micro-batch
      .option("fetchOffset.numRetries", "3")                  // retry offset fetch
      .option("minPartitions", "5")                           // split partitions for parallelism
      .option("kafka.group.id", "spark-secure-consumer")      // consumer group

      // -------------------------
      // Security (SSL / SASL_SSL)
      // -------------------------
      .option("kafka.security.protocol", "SASL_SSL")          // SSL or SASL_SSL
      .option("kafka.sasl.mechanism", "PLAIN")                // PLAIN / SCRAM-SHA-512
      .option("kafka.sasl.jaas.config",
        """org.apache.kafka.common.security.plain.PlainLoginModule required
           username="kafka-user"
           password="kafka-pass";""")

      // TLS / SSL truststore & keystore configs
      .option("kafka.ssl.truststore.location", "/etc/security/kafka.client.truststore.jks")
      .option("kafka.ssl.truststore.password", "truststore-pass")
      .option("kafka.ssl.keystore.location", "/etc/security/kafka.client.keystore.jks")
      .option("kafka.ssl.keystore.password", "keystore-pass")
      .option("kafka.ssl.key.password", "key-pass")

      // Optional SSL settings
      .option("kafka.ssl.endpoint.identification.algorithm", "https") // set "" to disable hostname verification
      .load()

    // -------------------------
    // Deserialize Avro using Schema Registry
    // -------------------------
    val avroSchema =
      """
      {
        "type": "record",
        "name": "Customer",
        "fields": [
          {"name": "id", "type": "int"},
          {"name": "name", "type": "string"},
          {"name": "email", "type": "string"}
        ]
      }
      """

    val avroDF = kafkaDF
      .select(from_avro(col("value"), avroSchema, Map(
        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> schemaRegistryUrl
      )).as("data"))
      .select("data.*")

    // -------------------------
    // Write stream sink
    // -------------------------
    val query = avroDF.writeStream
      .format("console") // or "kafka", "delta", "parquet", "jdbc"
      .outputMode("append")
      .option("truncate", false)
      .option("checkpointLocation", "/tmp/checkpoints/kafka_secure_stream")
      .start()

    query.awaitTermination()
  }
}
