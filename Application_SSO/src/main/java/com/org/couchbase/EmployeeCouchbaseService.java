package com.org.couchbase;

import java.util.List;
import org.springframework.data.couchbase.core.query.N1qlPrimaryIndexed;
import org.springframework.data.couchbase.core.query.ViewIndexed;
import com.org.dto.EmployeeDto;

@N1qlPrimaryIndexed
@ViewIndexed(designDoc = "EmployeeDocument", viewName = "EmployeeAll")
public interface EmployeeCouchbaseService {

	EmployeeDto findOne(String id);

	List<EmployeeDto> findAll();

	List<EmployeeDto> findByFirstName(String firstName);

	void create(EmployeeDto employeeDto);

	void update(EmployeeDto employeeDto);

	void delete(EmployeeDto employeeDto);
}
