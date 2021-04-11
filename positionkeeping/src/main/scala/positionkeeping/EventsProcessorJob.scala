/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
