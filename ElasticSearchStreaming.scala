import akka.NotUsed
import akka.stream.scaladsl.{Flow, Source}
import com.sksamuel.elastic4s._
import com.sksamuel.elastic4s.ElasticDsl._
import play.api.Logger

trait ElasticSearchStreaming {

  def client: ElasticClient

  def logger: Logger

  def bulkIndexFlow(parallel: Int = 2, batchSize: Int = 2000): Flow[IndexDefinition, BulkResult, NotUsed] = {
  
    Flow[IndexDefinition]
      .grouped(batchSize)
      .mapAsyncUnordered(parallel)( batch =>
        client.execute(bulk(batch: _*))
      ).map{ res =>
        logger.trace(s"Bulk indexed ${res.items.length} documents")
        if (res.hasFailures) logger.warn(res.failureMessage)
        res
      }
  }

  def scrollQuerySource[A](query: SearchDefinition, keepAlive: String = "10s")
                          (implicit hitAs: HitAs[A]): Source[A, NotUsed] = {

    logger.trace(s"Streaming $query")

    val futureFirstPage = client.execute(query.scroll(keepAlive))

    val futureSrc = futureFirstPage.map{ firstPage =>
      val firstPageSrc = Source(firstPage.as[A].toVector)
      val nextPagesSrc = Source.unfoldAsync(firstPage.scrollId){ scrollId =>
        client.execute(
          searchScroll(scrollId).keepAlive(keepAlive)
        ).map{ resp =>
          val docs = resp.as[A].toVector
          if (docs.isEmpty) None else Some(resp.scrollId -> docs)
        }
      }
      firstPageSrc ++ nextPagesSrc.mapConcat(identity)
    }

    Source.fromFuture(futureSrc).flatMapConcat(identity)
  }

}
