package com.org.controller;

import java.security.Principal;
import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.AnonymousAuthenticationToken;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import com.org.model.User;
import com.org.security.AuthenticationDetails;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Controller
@RequestMapping(value = "/request")
public class LoginController {

	@Autowired
	private AuthenticationDetails authenticationDetails;

	@Resource
	private AuthenticationManager authenticationManager;

	@RequestMapping(value = "/authenticate", method = RequestMethod.GET)
	public String login(@RequestParam String page) {
		
		if (page.equalsIgnoreCase("login")) {
			log.info("Request for Login Page");
			return "login";
		}

		log.info("Request for Logout Page");
		return "logout";
	}

	@RequestMapping(value = "/validateUser/access", method = RequestMethod.POST)
	public void authenticateUser(@RequestBody User user, HttpServletRequest httpServletRequest, Exception exception) {

		log.info("Validating End Users");
		UsernamePasswordAuthenticationToken authenticationToken = new UsernamePasswordAuthenticationToken(
				user.getUserName(), user.getPassword());
		Authentication authentication = authenticationManager.authenticate(authenticationToken);
		
		log.info("Authentication Token is set");
		SecurityContext securityContext = SecurityContextHolder.getContext();
		securityContext.setAuthentication(authentication);
		
		log.info("Session is set");
		HttpSession httpSession = httpServletRequest.getSession(true);
		httpSession.setAttribute("SPRING_SECURITY_CONTEXT", securityContext);
	}

	@RequestMapping(value = "/user/home", method = RequestMethod.GET)
	public String landinPage(@RequestParam String role) {

		if (role.equalsIgnoreCase("Admin")) {
			log.info("Login as Admin");
			return "console";
		}

		return "homepage";
	}

	@RequestMapping(value = "/validate/session", method = RequestMethod.GET)
	public String session(@RequestParam String session) {

		if (session.equalsIgnoreCase("expired")) {
			log.warn("Session Expired");
			return "expiredAccount";
		}
		
		log.warn("Invalid Session");
		return "invalidSession";
	}

	@RequestMapping(value = "/info/userDetails", method = RequestMethod.GET)
	public ResponseEntity<String> userDetails(HttpServletRequest httpServletRequest) {

		String response = null;
		Authentication authentication = authenticationDetails.getAuthentication();

		if (!(authentication instanceof AnonymousAuthenticationToken)) {
			response = "Name: " + authentication.getName() + " Password: " + authentication.getCredentials()
					+ " Authorized: " + authentication.getAuthorities();

			Principal principal = httpServletRequest.getUserPrincipal();
			log.info("UserName: " + principal.getName());

			log.info("Role: " + httpServletRequest.isUserInRole("WRITE_PRIVILEGE"));
		}

		HttpHeaders httpHeaders = new HttpHeaders();
		httpHeaders.add("Custom-add", "Data-Records");

		return new ResponseEntity<String>(response, httpHeaders, HttpStatus.OK);
	}

}
