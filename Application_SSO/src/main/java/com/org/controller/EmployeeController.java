package com.org.controller;

import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import com.org.couchbase.EmployeeCouchbaseService;
import com.org.dto.EmployeeDto;
import com.org.model.Employee;
import com.org.service.EmployeeService;
import com.org.kafka.MessageProducer;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Controller
@RequestMapping("/employee")
public class EmployeeController {

	@Autowired
	private EmployeeService employeeService;

	@Autowired
	private EmployeeCouchbaseService employeeCouchbaseService;

	@Autowired
	private MessageProducer messageProducer;

	@RequestMapping(method = RequestMethod.GET, path = "/employeePage")
	public String employeePage() {
		log.info("Adding new Employee Page");
		return "add-edit-employee";
	}

	@RequestMapping(method = RequestMethod.POST, path = "/saveEmployee")
	public ResponseEntity<String> saveEmployee(Employee employee) {

		EmployeeDto employeeDto = new EmployeeDto();
		employeeDto.setFirstName(employee.getFirstName());
		employeeDto.setLastName(employee.getLastName());
		employeeDto.setEmail(employee.getEmail());
		
		messageProducer.sendMessage("App", "Employee", "2");

		employeeService.saveEmployee(employeeDto);

		employeeCouchbaseService.create(employeeDto);

		return new ResponseEntity<String>("Success", HttpStatus.OK);
	}

	@RequestMapping(method = RequestMethod.GET, path = "/getAllEmployees")
	public String getAllEmployees(Model model) {

		List<EmployeeDto> list = employeeService.getAllEmployees();

		model.addAttribute("employees", list);
		return "list-employees";
	}

	/*
	 * @RequestMapping(path = { "/edit", "/edit/{id}" }) public String
	 * editEmployeeById(Model model, @PathVariable("id") Optional<Long> id) throws
	 * RecordNotFoundException { if (id.isPresent()) { EmployeeEntity entity =
	 * service.getEmployeeById(id.get()); model.addAttribute("employee", entity); }
	 * else { model.addAttribute("employee", new EmployeeEntity()); } return
	 * "add-edit-employee"; }
	 * 
	 * @RequestMapping(path = "/delete/{id}") public String deleteEmployeeById(Model
	 * model, @PathVariable("id") Long id) throws RecordNotFoundException {
	 * service.deleteEmployeeById(id); return "redirect:/"; }
	 */
}
