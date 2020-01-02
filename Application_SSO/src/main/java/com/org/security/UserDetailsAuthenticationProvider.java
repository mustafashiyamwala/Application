package com.org.security;

import java.util.HashSet;
import java.util.Set;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.InternalAuthenticationServiceException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.authentication.dao.AbstractUserDetailsAuthenticationProvider;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.authentication.WebAuthenticationDetails;
import com.org.service.LoginService;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Configuration
public class UserDetailsAuthenticationProvider extends AbstractUserDetailsAuthenticationProvider {

	@Value(value = "${ip.address.access}")
	private String ipAddress;

	private PasswordEncoder passwordEncoder;
	private LoginService loginService;
	private Set<String> whitelist = new HashSet<String>();

	public UserDetailsAuthenticationProvider(PasswordEncoder passwordEncoder, LoginService loginService) {
		this.passwordEncoder = passwordEncoder;
		this.loginService = loginService;
	}

	public void validIpAddress() {

		this.ipAddress = "11.11.11.11, 0:0:0:0:0:0:0:1, 127.0.0.1";
		String[] ipAddress = this.ipAddress.split(",");

		for (String ip : ipAddress) {
			this.whitelist.add(ip.trim());
		}
		log.info("Authenticate IP Address Allow to Access");
	}

	@Override
	protected UserDetails retrieveUser(String username, UsernamePasswordAuthenticationToken authentication)
			throws AuthenticationException {
		// TODO Auto-generated method stub

		UserDetails userDetails;
		try {
			userDetails = this.loginService.loadUserByUsername(authentication.getPrincipal().toString());
			log.info("Fetching Specific End User Credentials for Authentication and Authorization");

		} catch (Exception e) {
			// TODO: handle exception
			log.error(e.getMessage());
			throw new InternalAuthenticationServiceException(e.getMessage(), e);
		}

		if (userDetails == null) {
			log.error("UserDetailsService returned null, which is an interface contract violation");
			throw new InternalAuthenticationServiceException(
					"UserDetailsService returned null, which is an interface contract violation");
		}
		return userDetails;
	}

	@Override
	protected void additionalAuthenticationChecks(UserDetails userDetails,
			UsernamePasswordAuthenticationToken authentication) throws AuthenticationException {
		// TODO Auto-generated method stub

		log.info("Validating the End Users Credentials");

		WebAuthenticationDetails webAuthenticationDetails = (WebAuthenticationDetails) authentication.getDetails();
		String ip = webAuthenticationDetails.getRemoteAddress();

		validIpAddress();

		if (!whitelist.contains(ip)) {
			log.error("Invalid IP Address");
			throw new BadCredentialsException("Invalid IP Address");
		}

		if (authentication.getCredentials() == null) {
			log.error("Authentication failed: no credentials provided");
			throw new BadCredentialsException(
					messages.getMessage("AbstractUserDetailsAuthenticationProvider.badCredentials", "Bad credentials"));
		}

		String password = authentication.getCredentials().toString();
		if (!passwordEncoder.matches(password, userDetails.getPassword())) {
			log.error("Authentication failed: password does not match stored value");
			throw new BadCredentialsException(
					messages.getMessage("AbstractUserDetailsAuthenticationProvider.badCredentials", "Bad credentials"));
		}
	}
}
