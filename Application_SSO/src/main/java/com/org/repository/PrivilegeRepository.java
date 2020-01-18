package com.org.repository;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;
import com.org.entity.PrivilegeEntity;

@Repository
public interface PrivilegeRepository extends CrudRepository<PrivilegeEntity, Long> {

	public PrivilegeEntity findByName(String name);
}
