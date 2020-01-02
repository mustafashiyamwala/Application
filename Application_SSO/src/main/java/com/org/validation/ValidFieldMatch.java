package com.org.validation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import javax.validation.Constraint;
import javax.validation.Payload;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Constraint(validatedBy = FieldMatchValidation.class)
public @interface ValidFieldMatch {

	public String message() default "Invalid Match";

	public String field();

	public String fieldMatch();

	public Class<?>[] groups() default {};

	public Class<? extends Payload>[] payload() default {};

	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.TYPE)
	public @interface List {
		ValidFieldMatch[] value();
	}
}
