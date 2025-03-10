import com.google.cloud.storage.{Blob, Storage, StorageOptions}
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream}
import java.nio.charset.StandardCharsets
import java.security._
import java.security.cert.{CertificateFactory, X509Certificate}
import java.security.spec.PKCS8EncodedKeySpec
import java.util.Base64
import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}

object SSLContextFromGCSPEM {
  def main(args: Array[String]): Unit = {
    val gcsPath = "gs://your-bucket-name/path/to/your-cert.pem"

    // Extract bucket and object path
    val (bucketName, objectPath) = extractBucketAndPath(gcsPath)

    // Read the PEM file from GCS
    val storage: Storage = StorageOptions.getDefaultInstance.getService
    val blob: Blob = storage.get(bucketName, objectPath)
    val pemContent: String = new String(blob.getContent(), StandardCharsets.UTF_8)

    // Extract Certificate and Private Key
    val (certificate, privateKey) = extractCertificateAndKey(pemContent)

    // Create SSLContext
    val sslContext = createSSLContext(certificate, privateKey)

    println("SSLContext successfully created.")
  }

  /** Extracts the bucket name and object path from a GCS URI */
  def extractBucketAndPath(gcsPath: String): (String, String) = {
    require(gcsPath.startsWith("gs://"), "Invalid GCS path. Must start with 'gs://'")
    val pathWithoutPrefix = gcsPath.stripPrefix("gs://")
    val parts = pathWithoutPrefix.split("/", 2)
    if (parts.length < 2) throw new IllegalArgumentException("Invalid GCS path format")
    (parts(0), parts(1)) // (bucketName, objectPath)
  }

  /** Extracts the X509 Certificate and Private Key from a PEM file */
  def extractCertificateAndKey(pemContent: String): (X509Certificate, PrivateKey) = {
    val certRegex = "-----BEGIN CERTIFICATE-----(.*?)-----END CERTIFICATE-----".r
    val keyRegex = "-----BEGIN PRIVATE KEY-----(.*?)-----END PRIVATE KEY-----".r

    val certBase64 = certRegex.findFirstMatchIn(pemContent).map(_.group(1).replaceAll("\\s", ""))
      .getOrElse(throw new IllegalArgumentException("No valid certificate found"))
    val keyBase64 = keyRegex.findFirstMatchIn(pemContent).map(_.group(1).replaceAll("\\s", ""))
      .getOrElse(throw new IllegalArgumentException("No valid private key found"))

    // Decode Certificate
    val certBytes = Base64.getDecoder.decode(certBase64)
    val certFactory = CertificateFactory.getInstance("X.509")
    val certificate = certFactory.generateCertificate(new ByteArrayInputStream(certBytes)).asInstanceOf[X509Certificate]

    // Decode Private Key
    val keyBytes = Base64.getDecoder.decode(keyBase64)
    val keyFactory = KeyFactory.getInstance("RSA") // Assuming RSA key
    val privateKey = keyFactory.generatePrivate(new PKCS8EncodedKeySpec(keyBytes))

    (certificate, privateKey)
  }

  /** Creates an SSLContext from the certificate and private key */
  def createSSLContext(certificate: X509Certificate, privateKey: PrivateKey): SSLContext = {
    val keyStore = KeyStore.getInstance(KeyStore.getDefaultType)
    keyStore.load(null, null)

    // Create KeyStore Entry
    val keyPassword = "changeit".toCharArray // Use a secure password in production
    keyStore.setKeyEntry("private-key", privateKey, keyPassword, Array(certificate))

    // Initialize KeyManagerFactory
    val kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
    kmf.init(keyStore, keyPassword)

    // Initialize TrustManagerFactory
    val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    tmf.init(keyStore)

    // Create and Initialize SSLContext
    val sslContext = SSLContext.getInstance("TLS")
    sslContext.init(kmf.getKeyManagers, tmf.getTrustManagers, new SecureRandom())

    sslContext
  }
}



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
