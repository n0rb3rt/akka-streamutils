import akka.NotUsed
import akka.stream.scaladsl.{Flow, Source}
import com.datastax.driver.core._
import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}
import scala.language.implicitConversions

trait CassandraStreaming {

  def session: Session

  def executeStream(parallel: Int = 1): Flow[Statement, ResultSet, NotUsed] = {
    Flow[Statement].mapAsync(parallel){ session.executeAsync(_): Future[ResultSet] }
  }
  
  val scrollRows: Flow[ResultSet, Row, NotUsed] = {
    Flow[ResultSet].flatMapConcat{ rs =>
      val firstPage = Source(rs.take(rs.getAvailableWithoutFetching).toVector)
      val nextPages = Source.unfoldAsync(()){ _ =>
        if (rs.isExhausted) Future.successful(None)
        else rs.fetchMoreResults().map{ _ =>
          Some(() -> rs.take(rs.getAvailableWithoutFetching).toVector))
        }
      }
      firstPage ++ nextPages.mapConcat(identity)
    }
  }

  private implicit def futureToScala[A](lf: ListenableFuture[A]): Future[A] = {
    val p = Promise[A]()
    Futures.addCallback(lf, 
      new FutureCallback[A] {
        def onSuccess(a: A) = p.success(a)
        def onFailure(err: Throwable) = p.failure(err)
      }
    )
    p.future
  }

}

