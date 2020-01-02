package com.org.repository;

import java.util.Optional;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;
import com.org.entity.EmployeeEntity;

@Repository
public interface EmployeeRepository extends CrudRepository<EmployeeEntity, Long> {

	public Optional<EmployeeEntity> findByEmail(String name);
}
