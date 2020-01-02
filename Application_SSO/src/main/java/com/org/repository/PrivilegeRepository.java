package com.org.repository;

import org.springframework.data.repository.CrudRepository;
import com.org.entity.PrivilegeEntity;

public interface PrivilegeRepository extends CrudRepository<PrivilegeEntity, Long> {

	public PrivilegeEntity findByName(String name);
}
