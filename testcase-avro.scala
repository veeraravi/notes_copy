import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import io.confluent.kafka.schemaregistry.client.rest.RestService
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema as RestSchema
import org.apache.avro.Schema.Parser
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.avro.Schema

class SchemaRegistryFunSuite extends AnyFunSuite with Matchers {

  test("getSchemaFromSchemaRegistry should return the correct StructType schema") {
    // Arrange
    val mockRestService = mock(classOf[RestService], RETURNS_DEEP_STUBS)
    val inputTopic = "test-topic"
    val schemaSubjectId = "-value"
    val topicValueName = inputTopic + schemaSubjectId

    // Mock the RestSchema to return a schema string when getSchema() is called
    val mockRestResponseSchema = mock(classOf[RestSchema])
    val avroSchemaString = "{\"type\":\"record\",\"name\":\"Test\",\"fields\":[{\"name\":\"field1\",\"type\":\"string\"}]}"
    
    // Ensure that mockRestResponseSchema.getSchema returns the Avro schema string
    when(mockRestService.getLatestVersion(anyString())).thenReturn(mockRestResponseSchema)
    when(mockRestResponseSchema.getSchema).thenReturn(avroSchemaString)

    // Mock the Avro Parser to parse the schema string
    val mockAvroSchema = new Parser().parse(avroSchemaString)

    // Mock SchemaConverters.toSqlType to return a StructType
    val mockStructType = mock(classOf[StructType])
    when(SchemaConverters.toSqlType(mockAvroSchema).dataType.asInstanceOf[StructType]).thenReturn(mockStructType)

    // Act
    val schema = getSchemaFromSchemaRegistry(mockRestService, inputTopic)

    // Assert
    schema shouldBe a[StructType]
    // Check that the schema contains the expected field name
    schema.fieldNames should contain ("field1")
  }
}
-----------------------------------------------------
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.mockito.Mockito._
import org.apache.spark.sql.{DataFrame, SparkSession, Row}
import org.apache.spark.sql.types.{StructField, StructType, StringType, IntegerType, LongType}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.avro.generic.GenericRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.avro.SchemaConverters
import io.confluent.kafka.schemaregistry.client.rest.RestService
import org.apache.spark.sql.Column
import org.apache.avro.Schema

class AvroMessageProcessingTest extends AnyFunSuite with Matchers {

  test("getValidAvroMessages should process messages correctly and return a DataFrame") {
    // Arrange
    val spark = mock(classOf[SparkSession])
    val inputTopic = "test-topic"
    val sourceSchemaPath = "/path/to/schema"
    val schemaRegistryURL = "http://localhost:8081"
    val badRecordTargetLoc = "/bad/records"
    val consumerGroupId = "test-consumer-group"
    val datacenter = "us-west-1"
    val loadWholeMsgInCol = "true"

    // Mock the RDD[ConsumerRecord]
    val messagesRDD = mock(classOf[RDD[ConsumerRecord[String, GenericRecord]]])

    // Mock the RestService and the schema registry response
    val mockRestService = mock(classOf[RestService])
    val avroSchemaString = "{\"type\":\"record\",\"name\":\"Test\",\"fields\":[{\"name\":\"field1\",\"type\":\"string\"}]}"
    val mockAvroSchema = new Schema.Parser().parse(avroSchemaString)
    val schemaRegistrySchema = SchemaConverters.toSqlType(mockAvroSchema).dataType.asInstanceOf[StructType]

    when(mockRestService.getLatestVersion(anyString())).thenReturn(mock(classOf[io.confluent.kafka.schemaregistry.client.rest.entities.Schema]))
    when(mockRestService.getLatestVersion(anyString()).getSchema).thenReturn(avroSchemaString)

    // Mock the DCILauncherCommon.getSchemaFromFile method
    val mockCustomSchema = StructType(Seq(StructField("field1", StringType, nullable = true)))
    mockStatic(DCILauncherCommon.getClass)
    when(DCILauncherCommon.getSchemaFromFile(sourceSchemaPath)).thenReturn(mockCustomSchema)

    // Mock RDD transformations to create Row RDD
    val rowRDD = mock(classOf[RDD[Row]])
    when(messagesRDD.map(any())).thenReturn(rowRDD)

    // Define the expected DataFrame schema
    val expectedSchema = StructType(Seq(
      StructField("key", StringType, false),
      StructField("value", StringType, false),
      StructField("Kafka_topic", StringType, false),
      StructField("Kafka_partitionId", IntegerType, false),
      StructField("Kafka_offset", LongType, false),
      StructField("Kafka_CreateTime", LongType, false),
      StructField("timestampType", IntegerType, false)
    ))

    // Mock DataFrame creation
    val df = mock(classOf[DataFrame])
    when(spark.createDataFrame(rowRDD, expectedSchema)).thenReturn(df)

    // Mock schema enforcement using from_json and corrupt record handling
    val corruptRecordCol = mock(classOf[Column])
    val parsedJsonCol = mock(classOf[Column])
    val selectedDf = mock(classOf[DataFrame])

    when(df.withColumn("_corrupt_record", any[Column]())).thenReturn(df)
    when(df.withColumn("parsed_json", any[Column]())).thenReturn(df)
    when(df.select(any())).thenReturn(selectedDf)

    // Mock writing bad records if enabled
    doNothing().when(DCILauncherCommon).writeBadRecords(any[DataFrame], anyString())

    // Mock additional withColumn operations for consumerGroupId and datacenter
    when(selectedDf.withColumn("consumerGroupId", lit(consumerGroupId))).thenReturn(selectedDf)
    when(selectedDf.withColumn("data_center_region", lit(datacenter))).thenReturn(selectedDf)

    // Act
    val resultDF = getValidAvroMessages(
      spark,
      inputTopic,
      sourceSchemaPath,
      schemaRegistryURL,
      badRecordTargetLoc,
      consumerGroupId,
      datacenter,
      messagesRDD,
      loadWholeMsgInCol
    )

    // Assert
    verify(spark).createDataFrame(rowRDD, expectedSchema)
    resultDF should not be null
    resultDF.columns should contain allOf ("Kafka_topic", "Kafka_partitionId", "Kafka_offset", "consumerGroupId", "data_center_region")

    // Verify that bad records are written when the bad record location is enabled
    if (!badRecordTargetLoc.isEmpty) {
      verify(DCILauncherCommon, times(1)).writeBadRecords(any[DataFrame], eqTo(badRecordTargetLoc))
    }
  }
}


