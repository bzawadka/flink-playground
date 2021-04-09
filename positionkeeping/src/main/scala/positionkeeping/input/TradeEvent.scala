package positionkeeping.input

import positionkeeping.input.Currency.Currency
import positionkeeping.input.Side.Side

// this is a simplified example - an event should be more precise, an event log entry really, e.g. OrderPlacedEvent, TradeSubmittedEvent, ContractDefinedEvent, etc)
case class TradeEvent(tradeSide: Side,
                      dealtCurrency: Currency,
                      counterCurrency: Currency,
                      dealtCurrencyAmount: Double,
                      counterCurrencyAmount: Double,
                      rate: Double,
                      tradeDate: String,
                      productType: String,
                      counterparty: String,
                      trader: String,
                      broker: String)

object Side extends Enumeration {
  type Side = Value
  val BUY, SELL = Value
}

object Currency extends Enumeration {
  type Currency = Value
  val USD, EUR, PLN, HKD = Value
}
