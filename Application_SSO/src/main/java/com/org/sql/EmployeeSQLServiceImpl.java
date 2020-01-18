package com.org.sql;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.org.dao.EmployeeDao;
import com.org.dto.EmployeeDto;
import com.org.entity.EmployeeEntity;
import com.org.exeception.RegistrationException;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Service
public class EmployeeSQLServiceImpl implements EmployeeSQLService {

	@Autowired
	private EmployeeDao employeeDao;

	public EmployeeEntity save(EmployeeDto employeeDto) throws RegistrationException {

		EmployeeEntity entity = null;
		Optional<EmployeeEntity> employee = employeeDao.findByEmail(employeeDto.getEmail());

		if (employee == null) {
			EmployeeEntity employeeEntity = new EmployeeEntity();
			employeeEntity.setFirstName(employeeDto.getFirstName());
			employeeEntity.setLastName(employeeDto.getLastName());
			employeeEntity.setEmail(employeeDto.getEmail());

			entity = employeeDao.save(employeeEntity);
		}

		if (entity.getId() != null) {
			log.info("Successfully Register Employee in SQL");
			return entity;

		} else {
			log.warn("Unable to Register Employee in SQL");
			throw new RegistrationException("Unable to Register Employee in SQL");
		}
	}

	public EmployeeEntity update(EmployeeDto employeeDto) throws RegistrationException {
		Optional<EmployeeEntity> employee = employeeDao.findByEmail(employeeDto.getEmail());

		EmployeeEntity entity = null;
		if (employee != null) {
			EmployeeEntity employeeEntity = employee.get();
			employeeEntity.setEmail(employeeDto.getEmail());
			employeeEntity.setFirstName(employeeDto.getFirstName());
			employeeEntity.setLastName(employeeDto.getLastName());

			entity = employeeDao.save(employeeEntity);
		}

		if (entity.getId() != null) {
			log.info("Successfully Update Employee in SQL");
			return entity;

		} else {
			log.warn("Unable to Update Employee in SQL");
			throw new RegistrationException("No employee record exist for given id");
		}

	}

	public List<EmployeeDto> getAll() {

		List<EmployeeDto> list = new ArrayList<EmployeeDto>();
		List<EmployeeEntity> result = (List<EmployeeEntity>) employeeDao.findAll();

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

	public EmployeeDto getById(Long id) throws RegistrationException {

		Optional<EmployeeEntity> employee = employeeDao.findById(id);

		if (employee != null) {
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

	public void deleteById(Long id) throws RegistrationException {
		Optional<EmployeeEntity> employee = employeeDao.findById(id);

		if (employee != null) {
			employeeDao.deleteById(id);
			log.error("Successfully Deleted Employee Record");

		} else {
			log.error("No employee record exist for given id");
			throw new RegistrationException("No employee record exist for given id");
		}
	}
}