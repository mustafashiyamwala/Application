package com.org.repository;

import org.springframework.data.repository.CrudRepository;
import com.org.entity.RegistrationEntity;

public interface RegistrationRepository extends CrudRepository<RegistrationEntity, Long> {

	public RegistrationEntity findByEmail(String email);
}
