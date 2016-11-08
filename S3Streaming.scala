import java.util.concurrent.TimeoutException

import akka.NotUsed
import akka.actor.ActorSystem
import akka.pattern.after
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.util.ByteString
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model._
import StreamUtils.Chunker
import Responses._
import org.apache.commons.io.IOUtils
import play.api.Logger
import play.api.libs.streams.Accumulator
import play.api.mvc.BodyParser
import play.api.mvc.BodyParsers.parse._
import play.api.mvc.MultipartFormData.FilePart
import play.core.parsers.Multipart._

import scala.collection.JavaConversions._
import scala.concurrent.Future
import scala.concurrent.duration._

abstract class S3Streaming (implicit actorSystem: ActorSystem) {

  def logger: Logger

  def client: AmazonS3Client

  def downloadParts(
   key: String,
   meta: ObjectMetadata,
   bucket: String,
   parallel: Int = 4,
   chunkSize: Int = 8 * 1024 * 1024
 ): Source[ByteString, NotUsed] = {

    val length = meta.getContentLength
    val count = Math.ceil(length / chunkSize.toDouble).toInt

    Source(1 to count).map { i =>
      logger.trace(s"Getting object $key part ($i/$count)")
      new GetObjectRequest(bucket, key).withRange(
        (i - 1) * chunkSize,
        if (i * chunkSize > length) length else i * chunkSize - 1
      )
    }
    .mapAsync(parallel){ req =>
      Future(client.getObject(req)).map{ obj =>
        val bytes = ByteString(IOUtils.toByteArray(obj.getObjectContent))
        obj.close()
        bytes
      }
    }
  }

  def getObjMetadata(
    key: String,
    version: Option[String] = None,
    bucket: String
  ): Future[ObjectMetadata] = Future {
    val req = new GetObjectMetadataRequest(bucket, key)
    version.foreach(req.setVersionId)
    client.getObjectMetadata(req)
  }

  def initiateMultipart(key: String, bucket: String): Future[InitiateMultipartUploadResult] = Future {
    val initRequest = new InitiateMultipartUploadRequest(bucket, key)
    client.initiateMultipartUpload(initRequest)
  }

  def abortMultipart(
    key: String,
    uploadId: String,
    t: Throwable,
    bucket: String
  ): Future[CompleteMultipartUploadResult] = Future {

    val abort = new AbortMultipartUploadRequest(bucket, key, uploadId)
    client.abortMultipartUpload(abort)

  }.flatMap{ _ =>
    logger.error(s"Upload aborted [$bucket/$key]", t)
    Future.failed[CompleteMultipartUploadResult](t)
  }

  def completeMultipart(
    key: String,
    uploadId: String,
    parts: Seq[PartETag],
    bucket: String
  ): Future[CompleteMultipartUploadResult] = Future {

    val compRequest = new CompleteMultipartUploadRequest(bucket, key, uploadId, parts.toBuffer[PartETag])
    client.completeMultipartUpload(compRequest)

  }.map{ result =>
    logger.info(s"Completed uploading $key -- ${parts.size} parts")
    result
  }

  def partUploader(
    key: String,
    uploadId: String,
    bucket: String,
    parallel: Int = 4,
    chunkSize: Int = 8 * 1024 * 1024,
    retries: Int = 2,
    timeout: FiniteDuration = 1.minute
  ): Sink[ByteString, Future[Seq[PartETag]]] = {

    def uploadPart(idx: Int, bytes: ByteString, retryNum: Int, retries: Int): Future[PartETag] = {
      if (retryNum > retries) {
        val msg = s"Uploading part $idx failed for $key"
        logger.error(msg)
        Future.failed(new Exception(msg))
      }
      else {
        val uploadRequest = new UploadPartRequest()
          .withBucketName(bucket)
          .withKey(key)
          .withPartNumber(idx)
          .withUploadId(uploadId)
          .withInputStream(bytes.iterator.asInputStream)
          .withPartSize(bytes.size)

        val futureUpload = Future { client.uploadPart(uploadRequest).getPartETag }

        val futureTimeout = after(timeout, using = actorSystem.scheduler)(
          Future.failed[PartETag](new TimeoutException(s"Upload timeout after $timeout"))
        )

        Future.firstCompletedOf(Seq(futureUpload, futureTimeout))
          .recoverWith { case t: Throwable =>
            logger.warn(s"Try (${retryNum + 1}/${retries + 1}) failed for $key part $idx", t)
            uploadPart(idx, bytes, retryNum + 1, retries)
          }
      }
    }

    Flow[ByteString]
      .via(new Chunker(chunkSize))
      .zip(Source.fromIterator(() => Iterator.from(1)))
      .mapAsyncUnordered(parallel) { case (bytes, idx) =>
        logger.trace(s"Uploading $key chunk $idx -- ${bytes.size} bytes")
        uploadPart(idx, bytes, retryNum = 0, retries)
      }
      .toMat(Sink.seq[PartETag])(Keep.right)
  }

  def multipartBodyParser(key: String, bucket: String): BodyParser[CompleteMultipartUploadResult] = {
    BodyParser{ request =>
      Accumulator.flatten(
        initiateMultipart(key, bucket).map { init =>

          multipartParser(DefaultMaxTextLength, partHandler(key, init.getUploadId, bucket))
            .apply(request)
            .mapFuture {

              case Right(parts) =>
                completeMultipart(key, init.getUploadId, parts.files.head.ref, bucket)
                  .map { uploadResult => Right(uploadResult) }

              case Left(errResult) =>
                abortMultipart(key, init.getUploadId, new Exception(s"Transfer terminated for $key -- $errResult"), bucket)
                  .map { _ => Left(errResult) }

            }.recoverWith { case t: Throwable =>
              abortMultipart(key, init.getUploadId, t, bucket)
                .map { _ => Left(internalServerErrorResponse(new Exception(s"Uploading $key failed", t))) }
          }
        }
      )
    }
  }

  private def partHandler(key: String, uploadId: String, bucket: String): FilePartHandler[Seq[PartETag]] = {
    case FileInfo(partName, filename, contentType) =>
      Accumulator(partUploader(key, uploadId, bucket)).map{ parts =>
        FilePart(partName, filename, contentType, parts)
      }
  }

}

