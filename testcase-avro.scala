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

--------------------------------------------------------

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import java.util
import scala.collection.JavaConverters._
import org.slf4j.Logger
import org.mockito.Mockito.{mock, verify}

class KafkaUtilsTest extends AnyFunSuite with Matchers {

  // Mocking logger
  val logger: Logger = mock(classOf[Logger])

  test("CommittedoOffset should commit offsets correctly") {
    // Arrange
    val mockConsumer = mock(classOf[KafkaConsumer[String, String]])
    val topicName = "test-topic"
    val partition = 0
    val offset = 100L

    // Mock KafkaConsumer behavior
    val mockPartitionAndOffset = mock(classOf[util.HashMap[TopicPartition, OffsetAndMetadata]])
    
    // Act
    CommittedoOffset(mockConsumer, topicName, partition, offset)

    // Assert
    val topicPartition = new TopicPartition(topicName, partition)
    val offsetAndMetadata = new OffsetAndMetadata(offset)
    
    // Verify that commitSync was called with the right arguments
    verify(mockConsumer).commitSync(argThat { map: util.Map[TopicPartition, OffsetAndMetadata] =>
      map.size() == 1 && map.containsKey(topicPartition) && map.get(topicPartition) == offsetAndMetadata
    })
  }

  test("getValueFromPropertyFileOrDefault should return value from property file when present") {
    // Arrange
    val propertyFileMap = Map("key1" -> "value1", "key2" -> "value2")
    val consumerConfigKey = "consumer-key"
    val mockConsumerDefaultValues = mock(classOf[Map[String, String]])
    
    // Act
    val result = getValueFromPropertyFileOrDefault(propertyFileMap, "key1", consumerConfigKey)

    // Assert
    result shouldBe "value1"
  }

  test("getValueFromPropertyFileOrDefault should return default value when key is missing or empty") {
    // Arrange
    val propertyFileMap = Map("key1" -> "value1")
    val consumerConfigKey = "consumer-key"
    val mockConsumerDefaultValues = Map("consumer-key" -> "default-value")

    // Act
    val result = getValueFromPropertyFileOrDefault(propertyFileMap, "missingKey", consumerConfigKey)

    // Assert
    result shouldBe "default-value"
  }
}

=====================================

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import org.apache.spark.streaming.kafka010.OffsetRange
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.mockito.MockitoAnnotations
import org.slf4j.Logger

class OffsetUpdateTest extends AnyFunSuite with Matchers {

  // Mocking logger
  val logger: Logger = mock(classOf[Logger])

  // Mocking KafkaConsumer
  val consumer: KafkaConsumer[String, String] = mock(classOf[KafkaConsumer[String, String]])

  // Mocking OffsetRange
  val offsetRange1: OffsetRange = OffsetRange("test-topic-1", 0, 0L, 100L)
  val offsetRange2: OffsetRange = OffsetRange("test-topic-2", 1, 0L, 200L)
  val offsetRanges: Array[OffsetRange] = Array(offsetRange1, offsetRange2)

  // Mocking the CommittedOffset method
  def CommittedOffset(consumer: KafkaConsumer[String, String], topic: String, partition: Int, offset: Long): Unit = {
    // CommittedOffset mock implementation
  }

  // The method we are testing
  def updateOffset(): Unit = {
    logger.info("Committing the Offset to Kafka - Started")
    offsetRanges.foreach { offsetRange =>
      CommittedOffset(consumer, offsetRange.topic, offsetRange.partition, offsetRange.untilOffset)
    }
    logger.info("Committing the Offset to Kafka Completed")
    logger.info("Updating the email body with kafka topic details")
  }

  test("updateOffset should commit offsets correctly and log messages") {
    // Arrange
    // Mock CommittedOffset method
    val mockCommittedOffset = mock(classOf[CommittedOffset])
    
    // Act
    updateOffset()

    // Assert
    verify(logger).info("Committing the Offset to Kafka - Started")
    verify(mockCommittedOffset).apply(consumer, "test-topic-1", 0, 100L)
    verify(mockCommittedOffset).apply(consumer, "test-topic-2", 1, 200L)
    verify(logger).info("Committing the Offset to Kafka Completed")
    verify(logger).info("Updating the email body with kafka topic details")
  }
}
------------------------------------------------------------------------------
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import org.apache.spark.streaming.kafka010.OffsetRange
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.mockito.MockitoAnnotations
import org.slf4j.Logger

class OffsetCommitterTest extends AnyFunSuite with Matchers {

  // Mocking logger
  val logger: Logger = mock(classOf[Logger])

  // Mocking KafkaConsumer
  val consumer: KafkaConsumer[String, String] = mock(classOf[KafkaConsumer[String, String]])

  // Mocking OffsetRange
  val offsetRange1: OffsetRange = OffsetRange("test-topic-1", 0, 0L, 100L)
  val offsetRange2: OffsetRange = OffsetRange("test-topic-2", 1, 0L, 200L)
  val offsetRanges: Array[OffsetRange] = Array(offsetRange1, offsetRange2)

  // Test for updateOffset
  test("updateOffset should commit offsets correctly and log messages") {
    // Arrange
    val committedOffset = mock(classOf[(KafkaConsumer[String, String], String, Int, Long) => Unit])

    // Creating the instance of OffsetCommitter with the mocked committedOffset function
    val offsetCommitter = new OffsetCommitter(consumer, offsetRanges, committedOffset)

    // Act
    offsetCommitter.updateOffset()

    // Assert
    verify(committedOffset).apply(consumer, "test-topic-1", 0, 100L)
    verify(committedOffset).apply(consumer, "test-topic-2", 1, 200L)
    verify(logger).info("Committing the Offset to Kafka - Started")
    verify(logger).info("Committing the Offset to Kafka Completed")
    verify(logger).info("Updating the email body with kafka topic details")
  }
}
=========================================================
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeaders
import org.slf4j.Logger
import scala.collection.JavaConverters._

class KafkaMsgTest extends AnyFunSuite with Matchers {

  // Mocking logger
  val logger: Logger = mock(classOf[Logger])

  // Mocking HeaderjsonUtil function
  def HeaderjsonUtil(headersJson: String): String = {
    "," + headersJson
  }

  // Test for prepareKafkaMsg when headers are required
  test("prepareKafkaMsg should return valid message with headers when isKafkaHeaderRequired is true") {
    // Arrange
    val mockMessage = mock(classOf[ConsumerRecord[String, String]])
    val topicName = "test-topic"
    val partition = 0
    val offset = 100L
    val currTimestamp = 1633022820000L // Mock timestamp
    val messageValue = """{"field": "value"}"""
    
    // Mocking the Kafka message
    when(mockMessage.topic()).thenReturn(topicName)
    when(mockMessage.partition()).thenReturn(partition)
    when(mockMessage.offset()).thenReturn(offset)
    when(mockMessage.timestamp()).thenReturn(currTimestamp)
    when(mockMessage.value()).thenReturn(messageValue)

    // Mock Kafka headers
    val headers = new RecordHeaders()
    headers.add("headerKey", "headerValue".getBytes("UTF-8"))
    when(mockMessage.headers()).thenReturn(headers)

    // Act
    val result = prepareKafkaMsg(mockMessage, "Y", isKafkaHeaderRequired = true)

    // Assert
    result shouldBe """{"field": "value", "Kafka_topic":"test-topic","Kafka_offset":100,"Kafka_partitionId":0,"Kafka_CreateTime":1633022820000,"Kafka_header":{"headerKey":"headerValue"}, "Json":""{"field": "value"}"""
  }

  // Test for prepareKafkaMsg when headers are not required
  test("prepareKafkaMsg should return valid message without headers when isKafkaHeaderRequired is false") {
    // Arrange
    val mockMessage = mock(classOf[ConsumerRecord[String, String]])
    val topicName = "test-topic"
    val partition = 0
    val offset = 100L
    val currTimestamp = 1633022820000L // Mock timestamp
    val messageValue = """{"field": "value"}"""
    
    // Mocking the Kafka message
    when(mockMessage.topic()).thenReturn(topicName)
    when(mockMessage.partition()).thenReturn(partition)
    when(mockMessage.offset()).thenReturn(offset)
    when(mockMessage.timestamp()).thenReturn(currTimestamp)
    when(mockMessage.value()).thenReturn(messageValue)

    // Mock Kafka headers (not needed in this case)
    val headers = new RecordHeaders()
    when(mockMessage.headers()).thenReturn(headers)

    // Act
    val result = prepareKafkaMsg(mockMessage, "Y", isKafkaHeaderRequired = false)

    // Assert
    result shouldBe """{"field": "value", "Kafka_topic":"test-topic","Kafka_offset":100,"Kafka_partitionId":0,"Kafka_CreateTime":1633022820000, "Json":""{"field": "value"}"""
  }

  // Test for prepareKafkaMsg when message is null
  test("prepareKafkaMsg should return an empty string when the message is null") {
    // Act
    val result = prepareKafkaMsg(null, "Y", isKafkaHeaderRequired = true)

    // Assert
    result shouldBe ""
  }
}
======================================================

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.mockito.Mockito._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.Logger

class ParseXMLMessageTest extends AnyFunSuite with Matchers {

  // Mocking logger
  val logger: Logger = mock(classOf[Logger])

  // Test for parseXMLMessage with a valid message
  test("parseXMLMessage should return valid XML message with Kafka details") {
    // Arrange
    val mockMessage = mock(classOf[ConsumerRecord[String, String]])
    val topicName = "test-topic"
    val partition = 1
    val offset = 123L
    val currTimestamp = 1633022820000L
    val rowTag = "row"
    val messageValue = """<root><row>data</row></root>"""

    // Mocking the Kafka message
    when(mockMessage.topic()).thenReturn(topicName)
    when(mockMessage.partition()).thenReturn(partition)
    when(mockMessage.offset()).thenReturn(offset)
    when(mockMessage.timestamp()).thenReturn(currTimestamp)
    when(mockMessage.value()).thenReturn(messageValue)

    // Act
    val result = parseXMLMessage(mockMessage, rowTag, isKafkaHeaderRequired = true)

    // Assert
    result shouldBe """<root><row>data</row></root><Kafka_topic>test-topic</Kafka_topic><Kafka_offset>123</Kafka_offset><Kafka_partitionId>1</Kafka_partitionId><Kafka_CreateTime>1633022820000</Kafka_CreateTime></row>"""
  }

  // Test for parseXMLMessage with null message
  test("parseXMLMessage should return an empty string when message is null") {
    // Act
    val result = parseXMLMessage(null, "row", isKafkaHeaderRequired = true)

    // Assert
    result shouldBe ""
  }

  // Test for parseXMLMessage with an empty or malformed XML message
  test("parseXMLMessage should return empty string when the message does not contain the row tag") {
    // Arrange
    val mockMessage = mock(classOf[ConsumerRecord[String, String]])
    val topicName = "test-topic"
    val partition = 1
    val offset = 123L
    val currTimestamp = 1633022820000L
    val rowTag = "row"
    val messageValue = """<root><data>no-row-tag</data></root>"""

    // Mocking the Kafka message
    when(mockMessage.topic()).thenReturn(topicName)
    when(mockMessage.partition()).thenReturn(partition)
    when(mockMessage.offset()).thenReturn(offset)
    when(mockMessage.timestamp()).thenReturn(currTimestamp)
    when(mockMessage.value()).thenReturn(messageValue)

    // Act
    val result = parseXMLMessage(mockMessage, rowTag, isKafkaHeaderRequired = true)

    // Assert
    result shouldBe """<root><data>no-row-tag</data></root><Kafka_topic>test-topic</Kafka_topic><Kafka_offset>123</Kafka_offset><Kafka_partitionId>1</Kafka_partitionId><Kafka_CreateTime>1633022820000</Kafka_CreateTime></row>"""
  }
}
==========================================================
Here are the unit test cases for the getValidAvroMessages function based on the pattern of previous test cases. These tests cover both the happy path and error scenarios.

Unit Test Cases
Test Case 1: When messagesRDD is empty
scala
Copy code
test("getValidAvroMessages returns an empty DataFrame when messagesRDD is empty") {
  val spark = mock[SparkSession](ReturnsDeepStubs)
  val messagesRDD = mock[RDD[ConsumerRecord[String, GenericRecord]]]
  
  val sourceSchemaPath = "/path/to/schema.json"
  val schemaRegistryURL = "http://mock-schema-registry"
  val badRecordTargetLoc = "/path/to/bad/records"
  val consumerGroupId = "consumer-group-1"
  val datacenter = "datacenter-1"
  val loadWholeMsgInCol = "false"

  val varArgs = List(sourceSchemaPath, schemaRegistryURL, badRecordTargetLoc, consumerGroupId, datacenter, loadWholeMsgInCol)
  val emptyRDD = spark.sparkContext.emptyRDD[ConsumerRecord[String, GenericRecord]]

  when(messagesRDD.isEmpty()).thenReturn(true)
  when(spark.createDataFrame(emptyRDD, StructType(Nil))).thenReturn(mock[DataFrame])

  val result = getValidAvroMessages(spark, "test-topic", messagesRDD, varArgs)

  assert(result.isEmpty)
}
Test Case 2: When schemaRegistryURL is valid and messagesRDD is non-empty
scala
Copy code
test("getValidAvroMessages processes non-empty messagesRDD with a valid schema registry URL") {
  val spark = mock[SparkSession](ReturnsDeepStubs)
  val messagesRDD = mock[RDD[ConsumerRecord[String, GenericRecord]]]

  val sourceSchemaPath = "/path/to/schema.json"
  val schemaRegistryURL = "http://mock-schema-registry"
  val badRecordTargetLoc = "/path/to/bad/records"
  val consumerGroupId = "consumer-group-1"
  val datacenter = "datacenter-1"
  val loadWholeMsgInCol = "true"

  val varArgs = List(sourceSchemaPath, schemaRegistryURL, badRecordTargetLoc, consumerGroupId, datacenter, loadWholeMsgInCol)
  
  val rowRDD = spark.sparkContext.parallelize(Seq(Row("key1", "value1", "topic", 1, 1L, 1633046400000L, 1)))

  val mockSchemaRegistry = StructType(List(
    StructField("key", StringType, true),
    StructField("value", StringType, true),
    StructField("Kafka_topic", StringType, true),
    StructField("Kafka_partitionId", IntegerType, true),
    StructField("Kafka_offset", LongType, true),
    StructField("Kafka_CreateTime", LongType, true),
    StructField("timestampType", IntegerType, true)
  ))

  val mockDF = spark.createDataFrame(rowRDD, mockSchemaRegistry)

  when(messagesRDD.isEmpty()).thenReturn(false)
  when(spark.createDataFrame(rowRDD, mockSchemaRegistry)).thenReturn(mockDF)

  val result = getValidAvroMessages(spark, "test-topic", messagesRDD, varArgs)

  assert(result.schema == mockSchemaRegistry)
  assert(!result.isEmpty)
}
Test Case 3: When schemaRegistryURL is empty
scala
Copy code
test("getValidAvroMessages logs and returns DataFrame without parsing when schemaRegistryURL is empty") {
  val spark = mock[SparkSession](ReturnsDeepStubs)
  val messagesRDD = mock[RDD[ConsumerRecord[String, GenericRecord]]]

  val sourceSchemaPath = "/path/to/schema.json"
  val schemaRegistryURL = ""
  val badRecordTargetLoc = "/path/to/bad/records"
  val consumerGroupId = "consumer-group-1"
  val datacenter = "datacenter-1"
  val loadWholeMsgInCol = "false"

  val varArgs = List(sourceSchemaPath, schemaRegistryURL, badRecordTargetLoc, consumerGroupId, datacenter, loadWholeMsgInCol)
  val rowRDD = spark.sparkContext.parallelize(Seq(Row("key1", "value1", "topic", 1, 1L, 1633046400000L, 1)))

  val mockSchema = StructType(List(
    StructField("key", StringType, true),
    StructField("value", StringType, true)
  ))

  val mockDF = spark.createDataFrame(rowRDD, mockSchema)

  when(messagesRDD.isEmpty()).thenReturn(false)
  when(spark.createDataFrame(rowRDD, mockSchema)).thenReturn(mockDF)

  val result = getValidAvroMessages(spark, "test-topic", messagesRDD, varArgs)

  assert(result.schema == mockSchema)
}
Test Case 4: When isBadRecordLocEnabled is true
scala
Copy code
test("getValidAvroMessages writes bad records when isBadRecordLocEnabled is true") {
  val spark = mock[SparkSession](ReturnsDeepStubs)
  val messagesRDD = mock[RDD[ConsumerRecord[String, GenericRecord]]]

  val sourceSchemaPath = "/path/to/schema.json"
  val schemaRegistryURL = "http://mock-schema-registry"
  val badRecordTargetLoc = "/path/to/bad/records"
  val consumerGroupId = "consumer-group-1"
  val datacenter = "datacenter-1"
  val loadWholeMsgInCol = "false"

  val varArgs = List(sourceSchemaPath, schemaRegistryURL, badRecordTargetLoc, consumerGroupId, datacenter, loadWholeMsgInCol)
  val rowRDD = spark.sparkContext.parallelize(Seq(Row("key1", "corrupt_value")))

  val mockSchema = StructType(List(
    StructField("key", StringType, true),
    StructField("value", StringType, true)
  ))

  val mockDF = spark.createDataFrame(rowRDD, mockSchema)

  when(messagesRDD.isEmpty()).thenReturn(false)
  when(spark.createDataFrame(rowRDD, mockSchema)).thenReturn(mockDF)

  // Mock writing bad records
  val writeBadRecordsMethod = mock[Unit]
  when(writeBadRecordsMethod(mockDF, badRecordTargetLoc)).thenReturn(())

  val result = getValidAvroMessages(spark, "test-topic", messagesRDD, varArgs)

  assert(result.schema == mockSchema)
  verify(writeBadRecordsMethod).apply(mockDF, badRecordTargetLoc)
}


