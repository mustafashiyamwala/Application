package com.org.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import com.org.dto.EmployeeDto;
import com.org.service.EmployeeService;

public class Test {

	@Autowired
	private ApplicationContext _appContext;

	public static void main(String[] args)
			throws ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException,
			SecurityException, IllegalArgumentException, InvocationTargetException {
		// TODO Auto-generated method stub

		new Test().runn();

		Class<?> classObject = Class.forName("com.org.service.EmployeeService");

		Method method = classObject.getDeclaredMethod("saveEmployee", com.org.dto.EmployeeDto.class);

		Object obj = classObject.newInstance();

		EmployeeDto employeeDto = new EmployeeDto();
		method.invoke(obj, employeeDto);

		/*
		 * Field[] field = classObject.getDeclaredFields(); for (Field field2 : field) {
		 * 
		 * if (field2.getType().getName().equals("com.org.service.EmployeeService")) {
		 * System.out.println("in"); } }
		 */

		/*
		 * Constructor<?> constructor = classObject.getDeclaredConstructor(); Method
		 * method = classObject.getDeclaredMethod(methodName, object.getClass());
		 * 
		 * T obj = (T) constructor.newInstance();
		 */
	}

	public void runn() throws NoSuchMethodException, SecurityException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException {
		// _appContext = new AnnotationConfigApplicationContext(AnnotationConfig.class);

		EmployeeService bean = (EmployeeService) _appContext.getBean(EmployeeService.class);
		Method method = bean.getClass().getMethod("saveEmployee", com.org.dto.EmployeeDto.class);
		EmployeeDto employeeDto = new EmployeeDto();
		method.invoke(bean, employeeDto);
	}
}
