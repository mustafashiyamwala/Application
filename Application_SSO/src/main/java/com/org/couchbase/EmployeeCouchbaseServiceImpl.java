package com.org.couchbase;

import java.util.List;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.stereotype.Service;
import com.org.document.EmployeeDocument;
import com.org.dto.EmployeeDto;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Service
@Qualifier("EmployeeCouchbaseServiceImpl")
public class EmployeeCouchbaseServiceImpl implements EmployeeCouchbaseService {

	@Autowired
	private CouchbaseTemplate couchbaseTemplate;

	@Override
	public EmployeeDto findOne(String id) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<EmployeeDto> findAll() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<EmployeeDto> findByFirstName(String firstName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void create(EmployeeDto employeeDto) {
		// TODO Auto-generated method stub
		EmployeeDocument employeeDocument = new EmployeeDocument();
		employeeDocument.setFirstName(employeeDto.getFirstName());
		employeeDocument.setLastName(employeeDto.getLastName());
		employeeDocument.setEmail(employeeDto.getEmail());
		employeeDocument.setCreated(DateTime.now());
		employeeDocument.setUpdated(DateTime.now());

		couchbaseTemplate.insert(employeeDocument);

		log.info("Successfull Register Employee in CB");
	}

	@Override
	public void update(EmployeeDto employeeDto) {
		// TODO Auto-generated method stub

	}

	@Override
	public void delete(EmployeeDto employeeDto) {
		// TODO Auto-generated method stub

	}

}
