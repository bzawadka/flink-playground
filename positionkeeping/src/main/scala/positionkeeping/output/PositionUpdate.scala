package positionkeeping.output

import positionkeeping.input.Currency.Currency

case class PositionUpdate(currency: Currency,
                          amount: Double)
