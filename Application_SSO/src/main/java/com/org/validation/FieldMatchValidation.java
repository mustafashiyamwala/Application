package com.org.validation;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import org.springframework.beans.BeanWrapperImpl;

public class FieldMatchValidation implements ConstraintValidator<ValidFieldMatch, Object> {

	private String field;
	private String fieldMatch;

	@Override
	public void initialize(ValidFieldMatch constraintAnnotation) {
		// TODO Auto-generated method stub
		this.field = constraintAnnotation.field();
		this.fieldMatch = constraintAnnotation.fieldMatch();
	}

	@Override
	public boolean isValid(Object value, ConstraintValidatorContext context) {
		// TODO Auto-generated method stub

		Object fieldValue = new BeanWrapperImpl(value).getPropertyValue(this.field);
		Object fieldValueMatch = new BeanWrapperImpl(value).getPropertyValue(this.fieldMatch);

		if (fieldValue.equals(fieldValueMatch)) {
			return true;
		}
		
		return false;
	}

}
