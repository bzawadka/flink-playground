package positionkeeping

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import positionkeeping.input.{TradeEvent, TradeEventSource}
import positionkeeping.output.{PositionUpdate, PositionUpdateSink}

object EventsProcessorJob {

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration())

    val events: DataStream[TradeEvent] = env
      .addSource(new TradeEventSource)
      .name("events-source")

    val positionUpdates: DataStream[PositionUpdate] = events
      .keyBy(event => event.dealtCurrency)
      .process(new PositionAggregator)
      .name("position-aggregator")

    positionUpdates
      .addSink(new PositionUpdateSink)
      .name("risk-sink")

    env.execute("Events processor")
  }
}
