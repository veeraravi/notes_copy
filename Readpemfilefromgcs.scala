import com.google.cloud.storage.{Blob, Storage, StorageOptions}
import java.io.ByteArrayInputStream
import java.security.cert.{CertificateFactory, X509Certificate}
import java.nio.charset.StandardCharsets

object ReadPEMFromGCS {
  def main(args: Array[String]): Unit = {
    val gcsPath = "gs://your-bucket-name/path/to/your-cert.pem"

    // Extract bucket name and object path from GCS URI
    val (bucketName, objectPath) = extractBucketAndPath(gcsPath)

    // Initialize GCS Storage Client
    val storage: Storage = StorageOptions.getDefaultInstance.getService

    // Read the PEM file from GCS
    val blob: Blob = storage.get(bucketName, objectPath)
    val pemContent: String = new String(blob.getContent(), StandardCharsets.UTF_8)

    // Extract Certificate
    val certificate: X509Certificate = extractCertificate(pemContent)

    // Print Certificate Details
    println(s"Certificate Subject: ${certificate.getSubjectX500Principal}")
    println(s"Certificate Issuer: ${certificate.getIssuerX500Principal}")
    println(s"Certificate Expiry Date: ${certificate.getNotAfter}")
  }

  /** Extracts the bucket name and object path from a GCS URI */
  def extractBucketAndPath(gcsPath: String): (String, String) = {
    require(gcsPath.startsWith("gs://"), "Invalid GCS path. Must start with 'gs://'")
    val pathWithoutPrefix = gcsPath.stripPrefix("gs://")
    val parts = pathWithoutPrefix.split("/", 2)
    if (parts.length < 2) throw new IllegalArgumentException("Invalid GCS path format")
    (parts(0), parts(1)) // (bucketName, objectPath)
  }

  /** Extracts X509Certificate from PEM content */
  def extractCertificate(pemContent: String): X509Certificate = {
    val certContent = pemContent
      .replace("-----BEGIN CERTIFICATE-----", "")
      .replace("-----END CERTIFICATE-----", "")
      .replaceAll("\\s", "") // Remove whitespace

    val decodedBytes = java.util.Base64.getDecoder.decode(certContent)
    val certificateFactory = CertificateFactory.getInstance("X.509")
    val certStream = new ByteArrayInputStream(decodedBytes)
    certificateFactory.generateCertificate(certStream).asInstanceOf[X509Certificate]
  }
}
