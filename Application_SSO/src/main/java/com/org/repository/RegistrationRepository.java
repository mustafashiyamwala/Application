package com.org.repository;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;
import com.org.entity.RegistrationEntity;

@Repository
public interface RegistrationRepository extends CrudRepository<RegistrationEntity, Long> {

	public RegistrationEntity findByEmail(String email);
}
