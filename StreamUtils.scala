import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.util.ByteString

object StreamUtils {

  /**
    * An Akka Streams Flow that emits an element every time a value changes.
    * Useful for iterating a Cassandra table whenever the ID changes,
    * which for columns clustered by timestamp can be the most/least recent value.
    * @param getProperty a function on type E returning a value A that will be evaluated for changes
    * @tparam E
    * @tparam A
    */
  class EmitOnChanged[E, A](getProperty: E => A) extends GraphStage[FlowShape[E, E]] {
    val in = Inlet[E]("emitOnChanged.in")
    val out = Outlet[E]("emitOnChanged.out")
    val shape = FlowShape.of(in, out)

    override def createLogic(inheritedAttributes: Attributes) = new GraphStageLogic(shape) {
      private var prevElem: Option[E] = None

      setHandlers(in, out, new InHandler with OutHandler {
        override def onPush(): Unit = {
          val currElem: E = grab(in)
          val notChanged = prevElem.map(getProperty).contains(getProperty(currElem))
          if (notChanged) pull(in) else push(out, currElem)
          prevElem = Some(currElem)
        }
        override def onPull(): Unit = pull(in)
      })
    }
  }

  /**
    * An Akka Streams Flow that takes arbitrarily sized ByteStrings and rechunks them into ByteStrings of size 'chunkSize'
    * @param chunkSize the size of the chunks to output
    */
  class Chunker(chunkSize: Int) extends GraphStage[FlowShape[ByteString, ByteString]] {
    val in = Inlet[ByteString]("rechunk.in")
    val out = Outlet[ByteString]("rechunk.out")
    val shape = FlowShape.of(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
      private var buffer = ByteString.empty

      setHandlers(in, out, new InHandler with OutHandler {
        override def onPull(): Unit = {
          if (isClosed(in) || buffer.size >= chunkSize) emitChunk()
          else pull(in)
        }

        override def onPush(): Unit = {
          buffer ++= grab(in)
          emitChunk()
        }

        override def onUpstreamFinish(): Unit = {
          if (isAvailable(out)) emitChunk()
          if (buffer.isEmpty) completeStage()
        }
      })

      private def emitChunk(): Unit = {
        if (buffer.isEmpty) {
          if (isClosed(in)) completeStage()
          else pull(in)
        } else if (buffer.size < chunkSize && !isClosed(in)) {
          pull(in)
        } else {
          val (chunk, nextBuffer) = buffer.splitAt(chunkSize)
          buffer = nextBuffer
          push(out, chunk)
        }
      }

    }
  }

}
