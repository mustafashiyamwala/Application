package com.org.stream.aggregator;

import lombok.Data;

@Data
public class TradeStatistics {

	private String type;
	private String ticker;
	private int countTrades;
	private double sumPrice;
	private double minPrice;
	private double avgPrice;

	public TradeStatistics add(Trade trade) {

		if (trade.type == null || trade.ticker == null)
			throw new IllegalArgumentException("Invalid trade to aggregate: " + trade.toString());

		if (this.type == null)
			this.type = trade.type;

		if (this.ticker == null)
			this.ticker = trade.ticker;

		if (!this.type.equals(trade.type) || !this.ticker.equals(trade.ticker))
			throw new IllegalArgumentException("Aggregating stats for trade type " + this.type + " and ticker "
					+ this.ticker + " but recieved trade of type " + trade.type + " and ticker " + trade.ticker);

		if (countTrades == 0)
			this.minPrice = trade.price;

		this.countTrades = this.countTrades + 1;
		this.sumPrice = this.sumPrice + trade.price;
		this.minPrice = this.minPrice < trade.price ? this.minPrice : trade.price;

		return this;
	}

	public TradeStatistics computeAvgPrice() {
		this.avgPrice = this.sumPrice / this.countTrades;
		return this;
	}
}
