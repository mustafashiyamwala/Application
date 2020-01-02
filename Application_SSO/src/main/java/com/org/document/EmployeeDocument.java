package com.org.document;

import javax.validation.constraints.NotNull;
import org.hibernate.validator.constraints.Length;
import org.joda.time.DateTime;
import org.springframework.data.couchbase.core.mapping.Document;
import org.springframework.data.couchbase.core.mapping.id.GeneratedValue;
import org.springframework.data.couchbase.core.mapping.id.GenerationStrategy;
import com.couchbase.client.java.repository.annotation.Field;
import com.couchbase.client.java.repository.annotation.Id;
import lombok.Data;

@Data
@Document
public class EmployeeDocument {

	@Id
	@GeneratedValue(strategy = GenerationStrategy.UNIQUE)
	@Field("id")
	private String id;

	@Field
	@NotNull
	private String firstName;

	@Field
	@NotNull
	private String lastName;

	@Field
	@NotNull
	@Length(max = 200)
	private String email;

	@Field
	@NotNull
	private DateTime created;

	@Field
	private DateTime updated;
}
