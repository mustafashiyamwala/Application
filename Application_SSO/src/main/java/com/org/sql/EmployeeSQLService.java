package com.org.sql;

import java.util.List;
import com.org.dto.EmployeeDto;
import com.org.entity.EmployeeEntity;
import com.org.exeception.RegistrationException;

public interface EmployeeSQLService {

	public EmployeeEntity save(EmployeeDto employeeDto) throws RegistrationException;

	public EmployeeEntity update(EmployeeDto employeeDto) throws RegistrationException;

	public List<EmployeeDto> getAll();

	public EmployeeDto getById(Long id) throws RegistrationException;

	public void deleteById(Long id) throws RegistrationException;
}
