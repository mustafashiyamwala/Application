package com.org.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import com.org.dto.EmployeeDto;
import com.org.model.Employee;
import com.org.serializable.GenericSerializer;
import com.org.kafka.MessageProducer;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Controller
@RequestMapping("/employee")
public class EmployeeController {

	@Autowired
	private MessageProducer messageProducer;

	@Autowired
	private GenericSerializer genericSerializer;

	@Value(value = "${kafka.topicName}")
	private String topicName;

	@RequestMapping(method = RequestMethod.GET, path = "/employeePage")
	public String employeePage() {
		log.info("Adding new Employee Page");
		return "add-edit-employee";
	}

	@RequestMapping(method = RequestMethod.POST, path = "/saveEmployee")
	public ResponseEntity<String> saveEmployee(Employee employee) throws InterruptedException {

		EmployeeDto employeeDto = new EmployeeDto();
		employeeDto.setFirstName(employee.getFirstName());
		employeeDto.setLastName(employee.getLastName());
		employeeDto.setEmail(employee.getEmail());

		byte[] serializeData = genericSerializer.serialize(this.topicName, employeeDto);

		messageProducer.sendMessage(this.topicName, "Employee", serializeData);

		// employeeService.saveEmployee(employeeDto);

		// employeeCouchbaseService.create(employeeDto);

		return new ResponseEntity<String>("Success", HttpStatus.OK);
	}

	/*
	 * @RequestMapping(method = RequestMethod.GET, path = "/getAllEmployees") public
	 * String getAllEmployees(Model model) {
	 * 
	 * List<EmployeeDto> list = employeeService.getAllEmployees();
	 * 
	 * model.addAttribute("employees", list); return "list-employees"; }
	 */

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
