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
@Target(ElementType.FIELD)
@Constraint(validatedBy = EmailValidation.class)
public @interface ValidEmail {

	public String message() default "Invalid Email";

	public Class<?>[] groups() default {};

	public Class<? extends Payload>[] payload() default {};

}
