package positionkeeping.input

import positionkeeping.input.Currency.{EUR, HKD, PLN, USD}

import java.io.Serializable
import java.util

/**
 * Stubbed (hardcoded) events
 */
@SerialVersionUID(1L)
final class TradeEventIterator private(val bounded: Boolean) extends util.Iterator[TradeEvent] with Serializable {
  private var index = 0

  override def hasNext = if (index < TradeEventIterator.data.size) true
  else if (!bounded) {
    index = 0
    true
  }
  else false

  override def next = {
    val transaction = TradeEventIterator.data.get({
      index += 1;
      index - 1
    })
    transaction
  }
}

@SerialVersionUID(1L)
object TradeEventIterator {
  def bounded = new TradeEventIterator(true)

  def unbounded = new TradeEventIterator(false)

  private val data = util.Arrays.asList(
    TradeEvent(Side.BUY, EUR, USD, 1000, 1200, 1.2, "2020-04-01", "productA", "cA", "traderA", "brokerA"),
    TradeEvent(Side.BUY, EUR, USD, 1200, 1400, 1.2, "2020-04-01", "productB", "cB", "traderB", "brokerB"),
    TradeEvent(Side.BUY, USD, HKD, 1000, 8000, 8, "2020-04-01", "productA", "cA", "traderB", "brokerA"),
    TradeEvent(Side.BUY, HKD, PLN, 25000, 12200, 0.49, "2020-04-01", "productB", "cC", "traderC", "brokerC"),
    TradeEvent(Side.SELL, EUR, USD, 900, 1080, 1.2, "2020-04-01", "productA", "cB", "traderA", "brokerA"),
    TradeEvent(Side.SELL, EUR, USD, 1200, 1400, 1.2, "2020-04-01", "productB", "cA", "traderB", "brokerB"),
    TradeEvent(Side.SELL, USD, HKD, 800, 6400, 8, "2020-04-01", "productA", "cC", "traderC", "brokerA"),
    TradeEvent(Side.SELL, HKD, PLN, 20000, 9800, 0.49, "2020-04-01", "productB", "cB", "traderC", "brokerC")
  )
}
