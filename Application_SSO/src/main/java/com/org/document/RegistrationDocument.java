package com.org.document;

import javax.validation.constraints.NotNull;
import org.joda.time.DateTime;
import org.springframework.data.couchbase.core.mapping.Document;
import org.springframework.data.couchbase.core.mapping.id.GeneratedValue;
import org.springframework.data.couchbase.core.mapping.id.GenerationStrategy;
import com.couchbase.client.java.repository.annotation.Field;
import com.couchbase.client.java.repository.annotation.Id;
import lombok.Data;

@Data
@Document
public class RegistrationDocument {

	@Id
	@GeneratedValue(strategy = GenerationStrategy.UNIQUE)
	@Field("id")
	private String id;

	@Field
	@NotNull
	private String firstName;

	@Field
	private String lastName;

	@Field
	@NotNull
	private String email;

	@Field
	@NotNull
	private String password;

	@Field
	@NotNull
	private DateTime created;

	@Field
	private DateTime updated;

}
