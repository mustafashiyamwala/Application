package com.org.stream.join;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Search {
	private Integer userID;
	private String searchTerms;
}
