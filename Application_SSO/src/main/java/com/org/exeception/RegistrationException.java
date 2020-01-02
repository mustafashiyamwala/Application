package com.org.exeception;

public class RegistrationException extends Exception {

	private static final long serialVersionUID = 1L;

	public RegistrationException() {
		super();
	}

	public RegistrationException(String message) {
		super(message);
	}

	public RegistrationException(String message, Throwable throwable) {
		super(message, throwable);
	}

}
