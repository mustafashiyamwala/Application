package com.org.repository;

import org.springframework.data.repository.CrudRepository;
import com.org.entity.RoleEntity;

public interface RoleRepository extends CrudRepository<RoleEntity, Long> {

	public RoleEntity findByName(String name);
}
