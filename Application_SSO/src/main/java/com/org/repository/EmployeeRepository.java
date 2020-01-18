package com.org.repository;

import java.util.Optional;
import org.springframework.data.repository.CrudRepository;
import com.org.entity.EmployeeEntity;

public interface EmployeeRepository extends CrudRepository<EmployeeEntity, Long> {

	public Optional<EmployeeEntity> findByEmail(String email);
}
