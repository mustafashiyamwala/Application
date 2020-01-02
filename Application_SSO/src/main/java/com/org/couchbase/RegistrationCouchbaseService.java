package com.org.couchbase;

import java.util.List;
import org.springframework.data.couchbase.core.query.N1qlPrimaryIndexed;
import org.springframework.data.couchbase.core.query.ViewIndexed;
import com.org.dto.RegistrationDto;

@N1qlPrimaryIndexed
@ViewIndexed(designDoc = "RegistrationDocument", viewName = "RegistrationAll")
public interface RegistrationCouchbaseService {

	RegistrationDto findOne(String id);

	List<RegistrationDto> findAll();

	List<RegistrationDto> findByFirstName(String firstName);

	void create(RegistrationDto registrationDto);

	void update(RegistrationDto registrationDto);

	void delete(RegistrationDto registrationDto);
}
