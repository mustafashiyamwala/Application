package com.org.security;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.authentication.AuthenticationFailureHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class LoginFailureHandler implements AuthenticationFailureHandler {

	private ObjectMapper objectMapper = new ObjectMapper();

	@Override
	public void onAuthenticationFailure(HttpServletRequest request, HttpServletResponse response,
			AuthenticationException exception) throws IOException, ServletException {
		// TODO Auto-generated method stub
		
		response.setStatus(HttpStatus.UNAUTHORIZED.value());
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("timestamp", LocalDateTime.now());
		map.put("exception", exception.getMessage());
		
		log.error("Failed to Login");
		
		response.getOutputStream().println(objectMapper.writeValueAsString(map));
	}
}
