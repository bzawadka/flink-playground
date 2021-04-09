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

package spendreport

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.walkthrough.common.entity.{Alert, Transaction}

object FraudDetector {
  val SMALL_AMOUNT: Double = 1.00
  val LARGE_AMOUNT: Double = 500.00
  val DETECTION_PERIOD_IN_SECONDS: Long = 60 * 1000L
}

@SerialVersionUID(1L)
class FraudDetector extends KeyedProcessFunction[Long, Transaction, Alert] {

  @transient private var lastTransactionSuspiciousState: ValueState[java.lang.Boolean] = _
  @transient private var timerState: ValueState[java.lang.Long] = _

  @throws[Exception]
  override def open(parameters: Configuration): Unit = {
    val flagDescriptor = new ValueStateDescriptor("flag", Types.BOOLEAN)
    lastTransactionSuspiciousState = getRuntimeContext.getState(flagDescriptor)

    val timerDescriptor = new ValueStateDescriptor("timer-state", Types.LONG)
    timerState = getRuntimeContext.getState(timerDescriptor)
  }

  @throws[Exception]
  def processElement(
                      transaction: Transaction,
                      context: KeyedProcessFunction[Long, Transaction, Alert]#Context,
                      out: Collector[Alert]): Unit = {

    // a large transaction is only fraudulent if the previous one was small. Remembering information across events requires state.

    val lastTransactionWasSmall: Boolean = if (lastTransactionSuspiciousState.value == null) false else lastTransactionSuspiciousState.value()

    if (lastTransactionWasSmall) {
      if (transaction.getAmount > FraudDetector.LARGE_AMOUNT) {
        val alert = new Alert
        alert.setId(transaction.getAccountId)
        out.collect(alert)
      }

      lastTransactionSuspiciousState.clear()
    }

    if (transaction.getAmount < FraudDetector.SMALL_AMOUNT) {
      lastTransactionSuspiciousState.update(true)

      val timer = context.timerService.currentProcessingTime + FraudDetector.DETECTION_PERIOD_IN_SECONDS
      context.timerService.registerProcessingTimeTimer(timer)
      timerState.update(timer)
    }
  }

  override def onTimer(
                        timestamp: Long,
                        context: KeyedProcessFunction[Long, Transaction, Alert]#OnTimerContext,
                        out: Collector[Alert]): Unit = {
    val timer = timerState.value
    context.timerService.deleteProcessingTimeTimer(timer)

    timerState.clear()
    lastTransactionSuspiciousState.clear()
  }
}
