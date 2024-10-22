import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import io.confluent.kafka.schemaregistry.client.rest.RestService
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema
import org.apache.avro.Schema.Parser
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.catalyst.util.DataType
import org.mockito.Mockito.{RETURNS_DEEP_STUBS, mock}
import org.apache.avro.Schema

class SchemaRegistryFunSuite extends AnyFunSuite with Matchers {

  test("getSchemaFromSchemaRegistry should return the correct StructType schema") {
    // Arrange
    val mockRestService = mock(classOf[RestService], RETURNS_DEEP_STUBS)
    val inputTopic = "test-topic"
    val schemaSubjectId = "-value"

    // Mock Schema from the schema registry
    val mockRestResponseSchema = new Schema
    mockRestResponseSchema.setSchema("{\"type\":\"record\",\"name\":\"Test\",\"fields\":[{\"name\":\"field1\",\"type\":\"string\"}]}")

    // Mock the RestService to return the mock schema
    when(mockRestService.getLatestVersion(anyString())).thenReturn(mockRestResponseSchema)

    // Mock Avro Parser and Schema conversion
    val mockParser = mock(classOf[Parser], RETURNS_DEEP_STUBS)
    val mockAvroSchema = new Schema.Parser().parse(mockRestResponseSchema.getSchema)

    // Mock the Avro schema parsing
    when(mockParser.parse(anyString())).thenReturn(mockAvroSchema)

    // Mock SchemaConverters.toSqlType to return a StructType
    val mockStructType = mock(classOf[StructType])
    when(SchemaConverters.toSqlType(mockAvroSchema).dataType.asInstanceOf[StructType]).thenReturn(mockStructType)

    // Act
    val schema = getSchemaFromSchemaRegistry(mockRestService, inputTopic)

    // Assert
    schema shouldBe a[StructType]
    // Further assertions can be added here based on the expected fields
  }
}
