package com.org.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import javax.transaction.Transactional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;
import com.org.entity.PrivilegeEntity;
import com.org.entity.RegistrationEntity;
import com.org.entity.RoleEntity;
import com.org.repository.PrivilegeRepository;
import com.org.repository.RegistrationRepository;
import com.org.repository.RoleRepository;

@Component
public class InitialDataLoader implements ApplicationListener<ContextRefreshedEvent> {

	private boolean alreadySetUp = false;

	@Autowired
	private RegistrationRepository registrationRepository;

	@Autowired
	private RoleRepository roleRepository;

	@Autowired
	private PrivilegeRepository privilegeRepository;

	@Autowired
	private PasswordEncoder passwordEncoder;

	@Transactional
	@Override
	public void onApplicationEvent(ContextRefreshedEvent event) {
		// TODO Auto-generated method stub

		if (alreadySetUp) {
			return;
		}

		PrivilegeEntity readPrivilege = createPrivilegeIfNotFound("READ_PRIVILEGE");
		PrivilegeEntity writePrivilege = createPrivilegeIfNotFound("WRITE_PRIVILEGE");

		List<PrivilegeEntity> adminPrivileges = Arrays.asList(readPrivilege, writePrivilege);
		createRoleIfNotFound("ROLE_ADMIN", adminPrivileges);
		createRoleIfNotFound("ROLE_USER", Arrays.asList(readPrivilege));

		RoleEntity roleEntity = roleRepository.findByName("ROLE_ADMIN");
		
		RegistrationEntity registrationEntity = new RegistrationEntity();
		registrationEntity.setFirstName("test");
		registrationEntity.setLastName("test");
		registrationEntity.setPassword(passwordEncoder.encode("test"));
		registrationEntity.setEmail("test@test.com");
		registrationEntity.setRoles(Arrays.asList(roleEntity));
		registrationEntity.setEnabled(true);
		registrationRepository.save(registrationEntity);

		alreadySetUp = true;
	}

	@Transactional
	private RoleEntity createRoleIfNotFound(String name, Collection<PrivilegeEntity> privilegeEntities) {

		RoleEntity roleEntity = roleRepository.findByName(name);

		if (roleEntity == null) {
			roleEntity = new RoleEntity();
			roleEntity.setName(name);
			roleEntity.setPrivileges(privilegeEntities);
			roleRepository.save(roleEntity);
		}

		return roleEntity;
	}

	@Transactional
	private PrivilegeEntity createPrivilegeIfNotFound(String name) {

		PrivilegeEntity privilegeEntity = privilegeRepository.findByName(name);

		if (privilegeEntity == null) {
			privilegeEntity = new PrivilegeEntity();
			privilegeEntity.setName(name);
			privilegeRepository.save(privilegeEntity);
		}

		return privilegeEntity;
	}

}
