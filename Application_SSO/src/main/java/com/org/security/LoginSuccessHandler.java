package com.org.security;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.web.DefaultRedirectStrategy;
import org.springframework.security.web.RedirectStrategy;
import org.springframework.security.web.WebAttributes;
import org.springframework.security.web.authentication.AuthenticationSuccessHandler;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Setter
@Getter
public class LoginSuccessHandler implements AuthenticationSuccessHandler {

	private RedirectStrategy redirectStrategy = new DefaultRedirectStrategy();

	@Override
	public void onAuthenticationSuccess(HttpServletRequest request, HttpServletResponse response,
			Authentication authentication) throws IOException, ServletException {
		// TODO Auto-generated method stub

		String url = "/request/user/home?role=";

		Set<String> authority = new HashSet<String>();
		Collection<? extends GrantedAuthority> grantedAuthorities = authentication.getAuthorities();

		for (GrantedAuthority grantedAuthority : grantedAuthorities) {
			authority.add(grantedAuthority.getAuthority());
		}

		if (authority.contains("READ_PRIVILEGE") && authority.contains("WRITE_PRIVILEGE")) {
			url = url + "admin";

		} else if (authority.contains("READ_PRIVILEGE")) {
			url = url + "user";

		} else {
			log.error("Unable to get Authorization set for corresponding User");
			throw new IllegalStateException();
		}

		log.info("Authorization set for corresponding User");

		if (response.isCommitted()) {
			return;
		}

		redirectStrategy.sendRedirect(request, response, url);
		log.info("Redirected Successfully");

		HttpSession httpSession = request.getSession(false);
		if (httpSession == null) {
			return;
		}

		httpSession.removeAttribute(WebAttributes.AUTHENTICATION_EXCEPTION);
	}
}
