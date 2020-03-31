package com.org.stream.join;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class UserProfile {
	private Integer userID;
	private String userName;
	private String zipcode;
	private String[] interests;

	public UserProfile update(String zipcode, String[] interests) {
		this.zipcode = zipcode;
		this.interests = interests;
		return this;
	}
}
