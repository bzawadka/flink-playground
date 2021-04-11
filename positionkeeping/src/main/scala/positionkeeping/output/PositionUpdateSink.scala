package positionkeeping.output

import org.apache.flink.annotation.PublicEvolving
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.slf4j.LoggerFactory

/**
 * A sink for outputting position updates.
 */
@PublicEvolving
@SerialVersionUID(1L)
object PositionUpdateSink {
  private val LOG = LoggerFactory.getLogger(classOf[PositionUpdateSink])
}

@PublicEvolving
@SerialVersionUID(1L)
class PositionUpdateSink extends SinkFunction[PositionUpdate] {
  override def invoke(sourceEvent: PositionUpdate, context: SinkFunction.Context) = {
    val positionUpdateMsg = sourceEvent.currency + " position update: " + sourceEvent.amount
    PositionUpdateSink.LOG.info(positionUpdateMsg)
    System.out.println(positionUpdateMsg)
  }
}