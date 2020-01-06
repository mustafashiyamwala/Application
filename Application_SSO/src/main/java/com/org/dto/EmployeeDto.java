package com.org.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = false)
public class EmployeeDto extends GenericObjectDto {

	private String firstName;
	private String lastName;
	private String email;

	@Override
	public String toString() {
		return "{\"firstName\":\"" + firstName + "\", \"lastName\":\"" + lastName + "\", \"email\":\"" + email + "\"}";
	}

}
