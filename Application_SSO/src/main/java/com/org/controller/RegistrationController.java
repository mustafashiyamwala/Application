package com.org.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import com.org.couchbase.RegistrationCouchbaseService;
import com.org.dto.RegistrationDto;
import com.org.model.Registration;
import com.org.service.RegistrationService;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Controller
@RequestMapping(value = "/registration")
public class RegistrationController {

	@Autowired
	private RegistrationService registrationService;

	@Autowired
	private RegistrationCouchbaseService registrationCouchbaseService;

	@RequestMapping(method = RequestMethod.GET, value = "/loaduserdetails")
	public String loadRegistration() {
		log.info("Registration Form");
		return "registration";
	}

	@RequestMapping(method = RequestMethod.POST, value = "/saveuserdetails")
	public ResponseEntity<String> saveRegistrationDetails(Registration registration) {

		RegistrationDto registrationDto = new RegistrationDto();
		registrationDto.setFirstName(registration.getFirstName());
		registrationDto.setLastName(registration.getLastName());
		registrationDto.setEmail(registration.getEmail());
		registrationDto.setPassword(registration.getPassword());
		registrationDto.setMatchingPassword(registration.getMatchingPassword());

		registrationCouchbaseService.create(registrationDto);

		registrationService.saveRegistrationDetails(registrationDto);

		return new ResponseEntity<String>("Register", HttpStatus.OK);
	}
}
