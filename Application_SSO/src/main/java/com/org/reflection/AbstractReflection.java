package com.org.reflection;

import java.io.File;
import java.io.IOException;
//import java.lang.reflect.Constructor;
//import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
//import java.lang.reflect.Parameter;
import java.util.HashMap;
import java.util.Map;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.org.exeception.RegistrationException;
import com.org.util.ApplicationContextProvider;
//import com.org.model.Employee;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Component
public class AbstractReflection<T> {

	private static Map<Object, String> map = new HashMap<Object, String>();

	static {
		try {
			ObjectMapper objectMapper = new ObjectMapper();
			Services[] serviceObject = objectMapper.readValue(new File("src/main/resources/services.json"),
					Services[].class);

			for (Services service : serviceObject) {
				map.put(service.getObjectKey(), service.getServiceValue());
			}

			log.info("Loaded JSON Services File");

		} catch (IOException e) {
			// TODO Auto-generated catch block
			log.error("Unable to read Service JSON File " + e.getMessage());
		}
	}

	@SuppressWarnings("unchecked")
	public void callingMethod(T valueObject, String key, ApplicationContextProvider applicationContextProvider)
			throws RegistrationException {

		try {
			String methodName = actionEvent(valueObject);
			String className = map.get(key);

			Class<?> classObject = Class.forName(className);
			Method method = classObject.getDeclaredMethod(methodName, valueObject.getClass());

			T bean = (T) applicationContextProvider.getApplicationContext().getBean(classObject);

			method.invoke(bean, valueObject);

		} catch (ClassNotFoundException | NoSuchMethodException | SecurityException | IllegalAccessException
				| IllegalArgumentException | InvocationTargetException e) {
			// TODO Auto-generated catch block
			log.error("Unable to Reflection Class : " + e.getMessage());
			throw new RegistrationException("Unable to run Reflection Class : " + e.getMessage());

		}
	}

	public String actionEvent(T object) throws RegistrationException {
		String methodName = "save";

		try {
			JSONParser parser = new JSONParser();
			JSONObject jsonObject = (JSONObject) parser.parse(object.toString());
			methodName = (jsonObject.get("action") == null) ? methodName : (String) jsonObject.get("action");

		} catch (ParseException e) {
			// TODO Auto-generated catch block
			log.error("Unable to Parse " + e.getMessage());
			throw new RegistrationException("Unable to Parse " + e.getMessage());
		}

		return methodName;
	}

	public void usefulResource() {
		/*
		 * Employee employee = new Employee();
		 * 
		 * Class<?> employeeClass = employee.getClass(); Class<?> empClass =
		 * Class.forName("com.org.model.Employee"); Class<?> empSupClass =
		 * empClass.getSuperclass(); String className = empClass.getSimpleName(); int
		 * classModifier = employeeClass.getModifiers();
		 * 
		 * Class<?> empIntefaces[] = empClass.getInterfaces(); Package packages =
		 * empClass.getPackage();
		 * 
		 * Constructor<?> constructor[] = empClass.getConstructors(); Constructor<?>
		 * constructor2 = empClass.getConstructor(String.class, String.class,
		 * String.class); Parameter parameter[] = constructor2.getParameters();
		 * 
		 * Employee employee2 = (Employee) constructor2.newInstance("abc", "xyz",
		 * "abc@gmail,com");
		 * 
		 * Method method[] = empClass.getMethods(); Method method1 =
		 * empClass.getMethod("setFirstName"); Method method2 =
		 * empClass.getMethod("setFirstName", String.class);
		 * 
		 * Method declaredMethod[] = empClass.getDeclaredMethods(); Method
		 * declaredMethod1 = empClass.getDeclaredMethod("setFirstName"); Method
		 * declaredMethod2 = empClass.getDeclaredMethod("setFirstName", String.class);
		 * 
		 * boolean isAccessible2 = declaredMethod2.isAccessible();
		 * declaredMethod2.setAccessible(true); declaredMethod2.invoke(employee2,
		 * "Murtuza");
		 * 
		 * Field[] fields = empClass.getFields(); Field field2 =
		 * empClass.getField("firstName"); Class<?> fieldType = field2.getType(); String
		 * type = fieldType.getTypeName();
		 * 
		 * boolean isAccessible1 = field2.isAccessible(); field2.setAccessible(true);
		 * Object value = field2.get(employee); field2.set(employee, "lmn");
		 * 
		 * Field[] declareFields = empClass.getDeclaredFields(); Field declareField =
		 * empClass.getDeclaredField("firstName");
		 */
	}
}
