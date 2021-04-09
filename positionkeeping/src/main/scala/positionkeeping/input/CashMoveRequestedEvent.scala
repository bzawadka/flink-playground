package positionkeeping.input

import positionkeeping.input.Currency.Currency
import positionkeeping.input.Direction.Direction

case class CashMoveRequestedEvent(currency: Currency,
                                  amount: Double,
                                  tradeDate: String,
                                  settlementDate: String,
                                  tradeSide: Direction,
                                  book: String)

object Direction extends Enumeration {
  type Direction = Value
  val DEPOSIT, WITHDRAWAL = Value
}
