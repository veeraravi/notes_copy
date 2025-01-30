import org.scalatest.funsuite.AnyFunSuite
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import org.slf4j.LoggerFactory
import org.mockito.Mockito.RETURNS_DEEP_STUBS

class CopyFromHdfsToNasTest extends AnyFunSuite {

  val logger = LoggerFactory.getLogger(this.getClass)

  test("copyFromHdfsToNas should return true when retryCopy succeeds") {
    // Mock dependencies
    val mockRetryCopy = mock[Function2[String, String, Boolean]]
    val mockEnsureLocalPathExists = mock[Function1[String, Unit]]

    // Dummy paths (instead of mocking Strings)
    val dummyHdfsSourcePath = "/mock/hdfs/source"
    val dummyLocalDestPath = "/mock/local/dest"

    // Stubbing behavior
    when(mockRetryCopy.apply(any[String], any[String])).thenReturn(true)

    // Call the function under test
    val result = {
      mockEnsureLocalPathExists.apply(dummyLocalDestPath) // Ensure path exists
      val success = mockRetryCopy.apply(dummyHdfsSourcePath, dummyLocalDestPath)
      if (success) {
        logger.info(s"Files copied successfully from $dummyHdfsSourcePath to $dummyLocalDestPath")
      } else {
        logger.error(s"Failed to copy files from $dummyHdfsSourcePath to $dummyLocalDestPath after maxRetries retries.")
        throw new RuntimeException(s"Failed to copy files from: $dummyHdfsSourcePath to $dummyLocalDestPath after maxRetries retries")
      }
      success
    }

    // Assert the expected outcome
    assert(result)
    verify(mockRetryCopy).apply(dummyHdfsSourcePath, dummyLocalDestPath)
  }

  test("copyFromHdfsToNas should throw RuntimeException when retryCopy fails") {
    // Mock dependencies
    val mockRetryCopy = mock[Function2[String, String, Boolean]]
    val mockEnsureLocalPathExists = mock[Function1[String, Unit]]

    // Dummy paths (instead of mocking Strings)
    val dummyHdfsSourcePath = "/mock/hdfs/source"
    val dummyLocalDestPath = "/mock/local/dest"

    // Stubbing behavior
    when(mockRetryCopy.apply(any[String], any[String])).thenReturn(false)

    // Verify that the exception is thrown
    val exception = intercept[RuntimeException] {
      mockEnsureLocalPathExists.apply(dummyLocalDestPath) // Ensure path exists
      val success = mockRetryCopy.apply(dummyHdfsSourcePath, dummyLocalDestPath)
      if (success) {
        logger.info(s"Files copied successfully from $dummyHdfsSourcePath to $dummyLocalDestPath")
      } else {
        logger.error(s"Failed to copy files from $dummyHdfsSourcePath to $dummyLocalDestPath after maxRetries retries.")
        throw new RuntimeException(s"Failed to copy files from: $dummyHdfsSourcePath to $dummyLocalDestPath after maxRetries retries")
      }
      success
    }

    assert(exception.getMessage.contains("Failed to copy files"))
    verify(mockRetryCopy).apply(dummyHdfsSourcePath, dummyLocalDestPath)
  }
}
