package com.org.stream.aggregator;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Trade {

	String type;
	String ticker;
	double price;
	int size;
}
