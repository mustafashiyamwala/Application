package com.org.security;

import java.io.IOException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.springframework.security.core.Authentication;
import org.springframework.security.web.DefaultRedirectStrategy;
import org.springframework.security.web.RedirectStrategy;
import org.springframework.security.web.authentication.logout.LogoutSuccessHandler;
import org.springframework.security.web.authentication.logout.SimpleUrlLogoutSuccessHandler;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Setter
@Getter
public class LogoutSuccessHandlers extends SimpleUrlLogoutSuccessHandler implements LogoutSuccessHandler {

	private RedirectStrategy redirectStrategy = new DefaultRedirectStrategy();

	@Override
	public void onLogoutSuccess(HttpServletRequest request, HttpServletResponse response, Authentication authentication)
			throws IOException {
		// TODO Auto-generated method stub

		log.info("Logout Successfully" + request.getHeader("Referer"));
		// super.onLogoutSuccess(request, response, authentication);
		redirectStrategy.sendRedirect(request, response, "/request/authenticate?page=logout");

	}
}
