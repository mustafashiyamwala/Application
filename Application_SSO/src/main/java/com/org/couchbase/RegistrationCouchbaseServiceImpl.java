package com.org.couchbase;

import java.util.List;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.stereotype.Service;
import com.org.document.RegistrationDocument;
import com.org.dto.RegistrationDto;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Service
//@Qualifier("RegistrationCouchbaseServiceImpl")
public class RegistrationCouchbaseServiceImpl implements RegistrationCouchbaseService {

	@Autowired
	private CouchbaseTemplate couchbaseTemplate;

	@Override
	public RegistrationDto findOne(String id) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<RegistrationDto> findAll() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<RegistrationDto> findByFirstName(String firstName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void create(RegistrationDto registrationDto) {
		// TODO Auto-generated method stub
		RegistrationDocument registrationDocument = new RegistrationDocument();
		registrationDocument.setFirstName(registrationDto.getFirstName());
		registrationDocument.setLastName(registrationDto.getLastName());
		registrationDocument.setEmail(registrationDto.getEmail());
		registrationDocument.setPassword(registrationDto.getPassword());
		registrationDocument.setCreated(DateTime.now());
		registrationDocument.setUpdated(DateTime.now());

		couchbaseTemplate.insert(registrationDocument);
		
		log.info("Successfull Register User in CB");
	}

	@Override
	public void update(RegistrationDto registrationDto) {
		// TODO Auto-generated method stub

	}

	@Override
	public void delete(RegistrationDto registrationDto) {
		// TODO Auto-generated method stub

	}
}
