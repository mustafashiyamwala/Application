package com.org.custom.serde;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Customer {
	private int customerID;
	private String customerName;

}
