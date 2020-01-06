package com.org.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.validation.annotation.Validated;
import com.org.dto.EmployeeDto;
import com.org.entity.EmployeeEntity;
import com.org.exeception.RegistrationException;
import com.org.repository.EmployeeRepository;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Service
@Validated
public class EmployeeService {

	@Autowired
	private EmployeeRepository repository;

	public EmployeeEntity saveEmployee(@Valid EmployeeDto employeeDto) {

		EmployeeEntity entity = null;
		Optional<EmployeeEntity> employee = repository.findByEmail(employeeDto.getEmail());

		if (!employee.isPresent()) {
			EmployeeEntity employeeEntity = new EmployeeEntity();
			employeeEntity.setFirstName(employeeDto.getFirstName());
			employeeEntity.setLastName(employeeDto.getLastName());
			employeeEntity.setEmail(employeeDto.getEmail());

			entity = repository.save(employeeEntity);
		}

		if (entity.getId() == null) {
			log.warn("Unable to Register Employee in SQL");
		}

		log.info("Successfully Register Employee in SQL");
		
		

		return entity;
	}

	public EmployeeEntity updateEmployee(@Valid EmployeeDto employeeDto) {
		Optional<EmployeeEntity> employee = repository.findByEmail(employeeDto.getEmail());

		EmployeeEntity entity = null;
		if (employee.isPresent()) {
			EmployeeEntity employeeEntity = employee.get();
			employeeEntity.setEmail(employeeDto.getEmail());
			employeeEntity.setFirstName(employeeDto.getFirstName());
			employeeEntity.setLastName(employeeDto.getLastName());

			entity = repository.save(employeeEntity);
		}

		if (entity.getId() == null) {
			log.warn("Unable to Update Employee in SQL");
		}

		log.info("Successfully Update Employee in SQL");

		return entity;
	}

	public List<EmployeeDto> getAllEmployees() {

		List<EmployeeDto> list = new ArrayList<EmployeeDto>();
		List<EmployeeEntity> result = (List<EmployeeEntity>) repository.findAll();

		if (result.size() > 0) {
			for (EmployeeEntity employeeEntity : result) {
				EmployeeDto employeeDto = new EmployeeDto();
				employeeDto.setFirstName(employeeEntity.getFirstName());
				employeeDto.setLastName(employeeEntity.getLastName());
				employeeDto.setEmail(employeeEntity.getEmail());

				list.add(employeeDto);
			}
		}
		log.info("Successfully Listed All Employee Records");
		return list;
	}

	public EmployeeDto getEmployeeById(Long id) throws RegistrationException {

		Optional<EmployeeEntity> employee = repository.findById(id);

		if (employee.isPresent()) {
			EmployeeEntity employeeEntity = employee.get();

			EmployeeDto employeeDto = new EmployeeDto();
			employeeDto.setFirstName(employeeEntity.getFirstName());
			employeeDto.setLastName(employeeEntity.getLastName());
			employeeDto.setEmail(employeeEntity.getEmail());

			log.info("Successfully Listed Employee Records");
			return employeeDto;

		} else {
			log.error("No employee record exist for given id");
			throw new RegistrationException("No employee record exist for given id");
		}
	}

	public void deleteEmployeeById(Long id) throws RegistrationException {
		Optional<EmployeeEntity> employee = repository.findById(id);

		if (employee.isPresent()) {
			repository.deleteById(id);
			log.error("Successfully Deleted Employee Record");

		} else {
			log.error("No employee record exist for given id");
			throw new RegistrationException("No employee record exist for given id");
		}
	}
}