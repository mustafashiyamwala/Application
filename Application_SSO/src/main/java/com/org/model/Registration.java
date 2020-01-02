package com.org.model;

import lombok.Data;

@Data
public class Registration {

	private String firstName;
	private String lastName;
	private String email;
	private String password;
	private String matchingPassword;

}
