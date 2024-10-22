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

    // Mock the RestService response to return a schema
    val mockRestResponseSchema = new RestSchema(topicValueName, 1, 1, "{\"type\":\"record\",\"name\":\"Test\",\"fields\":[{\"name\":\"field1\",\"type\":\"string\"}]}", "AVRO")

    when(mockRestService.getLatestVersion(anyString())).thenReturn(mockRestResponseSchema)

    // Mock the Avro Parser to parse the schema
    val mockAvroSchema = new Parser().parse(mockRestResponseSchema.getSchema)

    // Mock SchemaConverters.toSqlType to return a StructType
    val mockStructType = mock(classOf[StructType])
    when(SchemaConverters.toSqlType(mockAvroSchema).dataType.asInstanceOf[StructType]).thenReturn(mockStructType)

    // Act
    val schema = getSchemaFromSchemaRegistry(mockRestService, inputTopic)

    // Assert
    schema shouldBe a[StructType]
    // Additional assertions for fields
    schema.fieldNames should contain allOf("field1")
  }
}

--------------------------------------------
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

    // Mock the RestService response to return a schema
    val mockRestResponseSchema = new RestSchema(topicValueName, 1, 1, "{\"type\":\"record\",\"name\":\"Test\",\"fields\":[{\"name\":\"field1\",\"type\":\"string\"}]}", "AVRO")

    when(mockRestService.getLatestVersion(anyString())).thenReturn(mockRestResponseSchema)

    // Mock the Avro Parser to parse the schema
    val mockAvroSchema = new Parser().parse(mockRestResponseSchema.getSchema)

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















