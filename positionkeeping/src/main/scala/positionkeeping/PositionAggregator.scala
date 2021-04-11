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
