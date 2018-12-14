package controllers

import java.io.File
import java.nio.file.Files
import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.alpakka.s3.scaladsl.MultipartUploadResult
import akka.stream.scaladsl._
import akka.util.ByteString
import javax.inject._
import org.talend.dataset.services.localconnection.S3Streamer
import play.api._
import play.api.data.Form
import play.api.data.Forms._
import play.api.libs.json.Json
import play.api.libs.streams._
import play.api.mvc.MultipartFormData.FilePart
import play.api.mvc._
import play.core.parsers.Multipart.FilePartHandler

import scala.concurrent.ExecutionContext

case class FormData(name: String)

/**
  * This controller handles a file upload.
  */
@Singleton
class HomeController @Inject()(cc: MessagesControllerComponents, configuration: Configuration)(
    implicit executionContext: ExecutionContext,
    system: ActorSystem,
    mat: Materializer)
    extends MessagesAbstractController(cc) {

  private val logger = Logger(this.getClass)

  val form = Form(
    mapping(
      "name" -> text
    )(FormData.apply)(FormData.unapply)
  )

  /**
    * Renders a start page.
    */
  def index = Action { implicit request =>
    Ok(views.html.index(form))
  }

   val filePartHandler: FilePartHandler[MultipartUploadResult] = info => {
    logger.debug("in filepart handler")
    val fileId = UUID.randomUUID
    val streamer = new S3Streamer(configuration)
    val sink = streamer.sink(fileId.toString)
    val chunkLogger = Flow[ByteString].map { chunk =>
      logger.debug(s"Chunk uploaded to S3 for file $fileId - size = ${chunk.size}")
      chunk
    }
    Accumulator(sink)
      .through(chunkLogger)
      .map(result => FilePart(s"$fileId", info.fileName, info.contentType, result))
  }

  /**
    * A generic operation on the temporary file that deletes the temp file after completion.
    */
  private def operateOnTempFile(file: File) = {
    val size = Files.size(file.toPath)
    logger.info(s"size = ${size}")
    Files.deleteIfExists(file.toPath)
    size
  }

  /**
    * Uploads a multipart file as a POST request.
    *
    * @return
    */
  def upload = Action(parse.multipartFormData(filePartHandler)) {
    implicit request =>
      request.body.files.headOption match {
        case None => Ok(Json.obj("fileId" -> "not found"))
        case Some(file) => Ok(Json.obj("fileId" -> file.key))
      }
  }

}
