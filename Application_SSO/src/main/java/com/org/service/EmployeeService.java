package com.org.service;

import javax.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.validation.annotation.Validated;
import com.org.couchbase.EmployeeCouchbaseService;
import com.org.dto.EmployeeDto;
import com.org.exeception.RegistrationException;
import com.org.sql.EmployeeSQLService;

@Service
@Validated
public class EmployeeService {

	@Autowired
	private EmployeeSQLService employeeSQLService;

	@Autowired
	private EmployeeCouchbaseService employeeCouchbaseService;

	public void save(@Valid EmployeeDto employeeDto) throws RegistrationException {

		employeeSQLService.save(employeeDto);
		employeeCouchbaseService.save(employeeDto);
	}

	public void update(@Valid EmployeeDto employeeDto) {

	}

	public void getAll() {

	}

	public void getById(Long id) throws RegistrationException {

	}

	public void deleteById(Long id) throws RegistrationException {

	}
}