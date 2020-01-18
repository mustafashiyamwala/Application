package com.org.couchbase;

import java.util.List;
import org.springframework.data.couchbase.core.query.N1qlPrimaryIndexed;
import org.springframework.data.couchbase.core.query.ViewIndexed;
import com.org.dto.EmployeeDto;
import com.org.exeception.RegistrationException;

@N1qlPrimaryIndexed
@ViewIndexed(designDoc = "EmployeeDocument", viewName = "EmployeeAll")
public interface EmployeeCouchbaseService {

	EmployeeDto getById(String id);

	List<EmployeeDto> getAll();

	List<EmployeeDto> findByFirstName(String firstName);

	void save(EmployeeDto employeeDto) throws RegistrationException;

	void update(EmployeeDto employeeDto);

	void delete(EmployeeDto employeeDto);
}
