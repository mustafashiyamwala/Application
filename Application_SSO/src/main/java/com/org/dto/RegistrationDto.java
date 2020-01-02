package com.org.dto;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import com.org.validation.ValidEmail;
import com.org.validation.ValidFieldMatch;
import lombok.Data;

@Data
@ValidFieldMatch.List(value = {
		@ValidFieldMatch(message = "Invalid Password", field = "password", fieldMatch = "matchingPassword") })
public class RegistrationDto {

	@NotNull
	@NotEmpty
	private String firstName;
	private String lastName;

	@NotNull
	@NotEmpty
	@ValidEmail
	private String email;

	@NotNull
	@NotEmpty
	private String password;

	@NotNull
	@NotEmpty
	private String matchingPassword;

}
