package services

import akka.stream.alpakka.s3.scaladsl.MultipartUploadResult
import akka.stream.scaladsl.Sink
import akka.util.ByteString

import scala.concurrent.Future

trait Streamer {

  def sink(fileId: String): Sink[ByteString, Future[MultipartUploadResult]]

}
