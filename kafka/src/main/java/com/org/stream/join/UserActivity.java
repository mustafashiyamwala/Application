package com.org.stream.join;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class UserActivity {
	private Integer userId;
	private String userName;
	private String zipcode;
	private String[] interests;
	private String searchTerm;
	private String page;

	public UserActivity updateSearch(String searchTerm) {
		this.searchTerm = searchTerm;
		return this;
	}

}
