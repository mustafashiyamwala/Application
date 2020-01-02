package com.org.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Service;
import com.org.entity.PrivilegeEntity;
import com.org.entity.RegistrationEntity;
import com.org.entity.RoleEntity;
import com.org.repository.RegistrationRepository;
import com.org.repository.RoleRepository;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Service
public class LoginService implements UserDetailsService {

	@Autowired
	private RegistrationRepository registrationRepository;

	@Autowired
	private RoleRepository roleRepository;

	@Override
	public UserDetails loadUserByUsername(String email) throws UsernameNotFoundException {
		// TODO Auto-generated method stub

		User userDetails;
		RegistrationEntity registrationEntity = registrationRepository.findByEmail(email);

		if (registrationEntity == null) {
			userDetails = new User("", "", true, true, true, true,
					getAuthorities(Arrays.asList(roleRepository.findByName("ROLE_USER"))));
			log.warn("No Information is Available for End Users");
			return userDetails;
		}

		log.info("User Information is Fetched");
		userDetails = new User(registrationEntity.getFirstName(), registrationEntity.getPassword(),
				registrationEntity.isEnabled(), true, true, true, getAuthorities(registrationEntity.getRoles()));

		return userDetails;
	}

	private Collection<? extends GrantedAuthority> getAuthorities(Collection<RoleEntity> roleEntities) {

		Collection<GrantedAuthority> authorities = new ArrayList<GrantedAuthority>();
		Collection<PrivilegeEntity> privilegeEntities = new ArrayList<PrivilegeEntity>();

		for (RoleEntity roleEntity : roleEntities) {
			privilegeEntities.addAll(roleEntity.getPrivileges());
		}

		for (PrivilegeEntity privilige : privilegeEntities) {
			authorities.add(new SimpleGrantedAuthority(privilige.getName()));
		}

		return authorities;
	}
}
