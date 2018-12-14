package org.talend.dataset.services.localconnection

import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import akka.stream.Materializer
import akka.stream.alpakka.s3.impl.ListBucketVersion2
import com.amazonaws.auth._
import com.amazonaws.regions.AwsRegionProvider
import javax.inject.{Inject, Singleton}
import akka.stream.alpakka.s3.{DiskBufferType, Proxy, S3Settings}
import akka.stream.alpakka.s3.scaladsl.{MultipartUploadResult, S3Client}
import akka.stream.scaladsl.Sink
import akka.util.ByteString
import play.api.{Configuration, Logger}
import services.Streamer

import scala.concurrent.{ExecutionContext, Future}

@Singleton
class S3Streamer @Inject()(configuration: Configuration)(
    implicit system: ActorSystem,
    mat: Materializer,
    ec: ExecutionContext)
    extends Streamer {

  Logger.info("IN S3STREAMER")
  //private val s3AlpakkaClient = alpakkaS3ClientBasic()
  private val bucket = "tdc-dataset-samples"
  private val tmpDir = "dataset/tmp"
  private val region = "us-east-1"
  private val host = "localhost"
  private val port = 8000

  def sink(fileId: String): Sink[ByteString, Future[MultipartUploadResult]] = {
    Logger.info("IN SINK")
    alpakkaS3ClientBasic().multipartUpload(bucket, s"$tmpDir/$fileId")
  }

  private def alpakkaS3Client(): S3Client = {
    Logger.debug("Building s3 client for endpoint")
    val credentialsProvider = new AWSStaticCredentialsProvider(
      new BasicAWSCredentials("YOUR KEY",
                              "YOUR SECRET"))
    val regionProvider = new AwsRegionProvider {
      lazy val getRegion: String = region
    }
    val proxy = Proxy(host, port, Uri.httpScheme(false))
    val diskBufferType = DiskBufferType(Paths.get("/tmp"))
    val s3Settings = new S3Settings(
      bufferType = diskBufferType,
      proxy = Some(proxy),
      credentialsProvider = credentialsProvider,
      s3RegionProvider = regionProvider,
      pathStyleAccess = true,
      endpointUrl = None,
      listBucketApiVersion = ListBucketVersion2
    )
    new S3Client(s3Settings)
  }

  private def alpakkaS3ClientBasic(): S3Client = {
    Logger.info("IN alpakkaS3ClientBasic")
    Logger.debug("Building s3 client from basic credentials without endpoint")
    val credentialsProvider = new AWSStaticCredentialsProvider(
      new BasicAWSCredentials("YOUR KEY",
                              "YOUR SECRET"))
    S3Client(credentialsProvider, region)
  }

}
