package org.sunbird.cloud.storage.service

import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.storage.{BlobId, BlobInfo, HttpMethod, Storage, StorageOptions}
import com.google.common.io.Files
import org.jclouds.ContextBuilder
import org.jclouds.blobstore.BlobStoreContext
import org.sunbird.cloud.storage.BaseStorageService
import org.sunbird.cloud.storage.Model.Blob
import org.sunbird.cloud.storage.exception.StorageServiceException
import org.sunbird.cloud.storage.factory.StorageConfig
import org.apache.tika.metadata.HttpHeaders
import org.apache.tika.mime.MimeTypes

import java.io.{ByteArrayInputStream, File, RandomAccessFile}
import java.util.concurrent.Executors
import scala.collection.JavaConverters._
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}

class GcloudStorageService(config: StorageConfig) extends BaseStorageService  {

  var context = ContextBuilder.newBuilder("google-cloud-storage").credentials(config.storageKey, config.storageSecret).buildView(classOf[BlobStoreContext])
  var blobStore = context.getBlobStore

  implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8))

  override def getPaths(container: String, objects: List[Blob]): List[String] = {
    objects.map{f => "gs://" + container + "/" + f.key}
  }

  private val gcpStorage: Storage = {
    val secret = config.storageSecret.trim
    val secretBytes =
      if (secret.startsWith("{")) {
        secret.getBytes("UTF-8")
      } else {
        java.util.Base64.getDecoder.decode(secret)
      }
    val creds = ServiceAccountCredentials.fromStream(new ByteArrayInputStream(secretBytes))
    StorageOptions.newBuilder().setProjectId(config.storageKey).setCredentials(creds).build().getService
  }

  // Overriding upload methos since multipart upload not working for GCP
  override def upload(bucket: String, file: String, objectKey: String, isDirectory: Option[Boolean] = Option(false), attempt: Option[Int] = Option(1), retryCount: Option[Int] = None, ttl: Option[Int] = None): String = {
    val fileObj = new File(file)
    if (isDirectory.get) {
      val d = new File(file)
      val files = filesList(d)
      val list = files.map { f =>
        val key = objectKey + f.getAbsolutePath.split(d.getAbsolutePath + File.separator).last
        upload(bucket, f.getAbsolutePath, key, Option(false), attempt, retryCount, ttl)
      }
      list.mkString(",")
    } else {
      parallelUpload(bucket, fileObj, objectKey)
    }
  }

  private def parallelUpload(bucket: String, fileObj: File, objectKey: String): String = {
    val fileSize = fileObj.length()

    def estimateBandwidthMbps(): Int = {
      val testData = Array.fill[Byte](1024 * 256)(0)
      val testName = s"$objectKey.bandwidth-test"
      val blobInfo = BlobInfo.newBuilder(BlobId.of(bucket, testName)).build()

      val start = System.nanoTime()
      gcpStorage.create(blobInfo, testData)
      val end = System.nanoTime()

      // cleanup
      try gcpStorage.delete(bucket, testName) catch { case _: Throwable => }

      val durationSec = (end - start).toDouble / 1e9
      val mbps = ((testData.length.toDouble * 8) / (1024 * 1024)) / durationSec
      mbps.toInt.max(1)
    }

    val bandwidthMbps = estimateBandwidthMbps()
    println(s"Estimated bandwidth: ~$bandwidthMbps Mbps")

    val (parallelism, minChunkMB) =
      if (bandwidthMbps <= 3) (4, 20)       // 3G
      else if (bandwidthMbps <= 20) (8, 10) // 4G
      else (32, 5)                          // Wi-Fi/5G

    implicit val ec: ExecutionContext =
      ExecutionContext.fromExecutor(Executors.newFixedThreadPool(parallelism))

    val dynamicChunkSize = Math.max(minChunkMB * 1024 * 1024, Math.ceil(fileSize.toDouble / 32).toLong).toInt
    val totalParts = Math.ceil(fileSize.toDouble / dynamicChunkSize).toInt

    println(s"Uploading $objectKey ($fileSize bytes) in $totalParts chunks of ~$dynamicChunkSize bytes with $parallelism parallelism...")

    def uploadWithRetry(blobInfo: BlobInfo, data: Array[Byte], retries: Int = 3): Unit = {
      var attempt = 0
      var done = false
      while (!done && attempt < retries) {
        try {
          gcpStorage.create(blobInfo, data)
          done = true
        } catch {
          case e: Exception =>
            attempt += 1
            println(s"Retrying upload for ${blobInfo.getName} (attempt $attempt)...")
            if (attempt == retries) throw e
        }
      }
    }
    val uploads: Future[Seq[String]] = Future.traverse((0 until totalParts).toList) { part =>
      Future {
        val offset = part * dynamicChunkSize
        val size = Math.min(dynamicChunkSize, (fileSize - offset).toInt)

        val raf = new RandomAccessFile(fileObj, "r")
        val buffer = new Array[Byte](size)
        try {
          raf.seek(offset)
          raf.readFully(buffer)
        } finally {
          raf.close()
        }

        val tempName = s"$objectKey.part$part"
        val blobInfo = BlobInfo.newBuilder(BlobId.of(bucket, tempName)).build()
        uploadWithRetry(blobInfo, buffer)
        println(s"Uploaded part $part -> $tempName ($size bytes)")
        tempName
      }
    }

    val composed = uploads.map { partNames =>
      def composeRecursive(parts: List[String], target: String): String = {
        if (parts.size <= 32) {
          gcpStorage.compose(
            Storage.ComposeRequest.newBuilder()
              .setTarget(BlobInfo.newBuilder(BlobId.of(bucket, target)).build())
              .addSource(parts.asJava)
              .build()
          )
          target
        } else {
          val grouped = parts.grouped(32).zipWithIndex.map { case (batch, idx) =>
            val intermediateName = s"$target.intermediate.$idx"
            gcpStorage.compose(
              Storage.ComposeRequest.newBuilder()
                .setTarget(BlobInfo.newBuilder(BlobId.of(bucket, intermediateName)).build())
                .addSource(batch.asJava)
                .build()
            )
            intermediateName
          }.toList
          composeRecursive(grouped, target)
        }
      }

      val finalObj = composeRecursive(partNames.toList, objectKey)
      println(s"Composed final object: $finalObj")

      // cleanup
      partNames.foreach(name => try gcpStorage.delete(bucket, name) catch { case _: Throwable => })
      s"https://storage.googleapis.com/$bucket/$objectKey"
    }

    Await.result(composed, 30.minutes)
  }

  override def listObjectKeys(bucket: String, prefix: String): List[String] = {
    val blobs = gcpStorage.list(bucket, Storage.BlobListOption.prefix(prefix))
    blobs.iterateAll().asScala.map(_.getName).toList
  }

  /**
   * Downloads a single file from GCP storage
   *
   * @param container The storage container/bucket name
   * @param objectKey The object key to download
   * @param destinationPath The full path where the file should be saved
   * @return true if download was successful, false otherwise
   */
  override def download(bucket: String, objectKey: String, localPath: String, isDirectory: Option[Boolean] = Option(false)): Unit = {
    if (isDirectory.get) {
      val objects = listObjectKeys(bucket, objectKey)
      val dir = new File(localPath)
      if (!dir.exists()) dir.mkdirs()
      objects.foreach { obj =>
        val relative = obj.stripPrefix(objectKey)
        val targetFile = new File(dir, relative)
        if (!targetFile.getParentFile.exists()) targetFile.getParentFile.mkdirs()
        downloadFile(bucket, obj, targetFile)
      }
    } else {
      val file = new File(localPath, objectKey.split("/").last)
      downloadFile(bucket, objectKey, file)
    }
  }

  private def downloadFile(bucket: String, objectKey: String, file: File): Unit = {
    val blob = gcpStorage.get(BlobId.of(bucket, objectKey))
    if (blob == null) throw new RuntimeException(s"Object $objectKey not found in $bucket")
    if (!file.getParentFile.exists()) file.getParentFile.mkdirs()
    blob.downloadTo(file.toPath)
    println(s"Downloaded $objectKey to ${file.getAbsolutePath}")
  }

  /**
   * Method to get V4 Signed URL when storage is GCP
   * @param re
   * @return
   */
  override def getPutSignedURL(bucket: String,
                               objectKey: String,
                               ttl: Option[Int],
                               permission: Option[String] = Option("r"),
                               contentType: Option[String] = Option("application/octet-stream"),
                               additionalParams: Option[Map[String, String]] = None): String = {

    val blobInfo = BlobInfo.newBuilder(BlobId.of(bucket, objectKey)).build()
    val expiry = Math.min(ttl.getOrElse(600), 604800) // max 7 days
    val url = gcpStorage.signUrl(
      blobInfo,
      expiry,
      java.util.concurrent.TimeUnit.SECONDS,
      Storage.SignUrlOption.httpMethod(HttpMethod.PUT),
      Storage.SignUrlOption.withV4Signature()
    )
    url.toString
  }

}