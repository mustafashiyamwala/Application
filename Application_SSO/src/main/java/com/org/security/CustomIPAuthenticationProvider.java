package com.org.security;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.web.authentication.WebAuthenticationDetails;
import org.springframework.stereotype.Component;

@Component
public class CustomIPAuthenticationProvider implements AuthenticationProvider {

	Set<String> whitelist = new HashSet<String>();

	public CustomIPAuthenticationProvider() {
		whitelist.add("11.11.11.11");
		whitelist.add("0:0:0:0:0:0:0:1");
	}

	@Override
	public Authentication authenticate(Authentication authentication) throws AuthenticationException {
		// TODO Auto-generated method stub
		
		WebAuthenticationDetails webAuthenticationDetails = (WebAuthenticationDetails) authentication.getDetails();
		String ip = webAuthenticationDetails.getRemoteAddress();

		if (!whitelist.contains(ip)) {
			throw new BadCredentialsException("Invalid IP Address");
		}

		String name = authentication.getName();
		String password = authentication.getCredentials().toString();

		if (name.equals("user") && password.equals("user1Pass")) {
			List<GrantedAuthority> authorities = new ArrayList<GrantedAuthority>();
			authorities.add(new SimpleGrantedAuthority("ROLE_USER"));
			return new UsernamePasswordAuthenticationToken(name, password, authorities);

		} else if (name.equals("admin") && password.equals("admin1Pass")) {
			List<GrantedAuthority> authorities = new ArrayList<GrantedAuthority>();
			authorities.add(new SimpleGrantedAuthority("ROLE_Admin"));
			return new UsernamePasswordAuthenticationToken(name, password, authorities);

		} else {
			throw new BadCredentialsException("Invalid username or password");

		}

	}

	@Override
	public boolean supports(Class<?> authentication) {
		// TODO Auto-generated method stub
		return authentication.equals(UsernamePasswordAuthenticationToken.class);
	}

}
