package com.org.security;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.springframework.security.authentication.AuthenticationServiceException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class AuthenticationFilters extends UsernamePasswordAuthenticationFilter {

	@Override
	public Authentication attemptAuthentication(HttpServletRequest httpServletRequest,
			HttpServletResponse httServletResponse) {

		if (!httpServletRequest.getMethod().equals("POST")) {
			log.warn("Authentication method not supported: " + httpServletRequest.getMethod());
			throw new AuthenticationServiceException(
					"Authentication method not supported: " + httpServletRequest.getMethod());
		}

		String userName = obtainUsername(httpServletRequest);
		String password = obtainPassword(httpServletRequest);

		if (userName == null) {
			userName = "";
		}

		if (password == null) {
			password = "";
		}

		UsernamePasswordAuthenticationToken authenticationToken = new UsernamePasswordAuthenticationToken(userName,
				password);
		setDetails(httpServletRequest, authenticationToken);

		return this.getAuthenticationManager().authenticate(authenticationToken);
	}
}
