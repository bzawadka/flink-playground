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

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import positionkeeping.input.Currency.Currency
import positionkeeping.input.{Side, TradeEvent}
import positionkeeping.output.PositionUpdate

@SerialVersionUID(1L)
class PositionAggregator extends KeyedProcessFunction[Currency, TradeEvent, PositionUpdate] {

  @transient private var balance: ValueState[java.lang.Double] = _

  @throws[Exception]
  override def open(parameters: Configuration): Unit = {
    val balanceDescriptor = new ValueStateDescriptor("balance", Types.DOUBLE)
    balance = getRuntimeContext.getState(balanceDescriptor)
  }

  @throws[Exception]
  def processElement(
                      event: TradeEvent,
                      context: KeyedProcessFunction[Currency, TradeEvent, PositionUpdate]#Context,
                      collector: Collector[PositionUpdate]): Unit = {

    val currentCurrency = context.getCurrentKey

    val positionChange = currentCurrency match {
      case event.dealtCurrency => event.dealtCurrencyAmount
      case event.counterCurrency => event.counterCurrencyAmount
      case _ => 0
    }

    if (positionChange > 0) {
      val previousBalance: Double = if (balance.value == null) 0 else balance.value
      val newBalance = event.tradeSide match {
        case Side.BUY => previousBalance + positionChange
        case Side.SELL => previousBalance - positionChange
      }
      balance.update(newBalance)

      val positionUpdate = PositionUpdate(currentCurrency, newBalance)
      collector.collect(positionUpdate)
    }
  }
}
