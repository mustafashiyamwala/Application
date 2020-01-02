package com.org.config;

import java.util.Arrays;
import java.util.List;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.repository.config.EnableCouchbaseRepositories;;

@Configuration
@EnableCouchbaseRepositories(basePackages = { "com.org" })
public class CouchbaseConfiguration extends AbstractCouchbaseConfiguration {

	@Value(value = "${couchbase.host.name}")
	private String hostName;
	
	@Value(value = "${couchbase.bucket.name}")
	private String bucketName;
	
	@Value(value = "${couchbase.bucket.password}")
	private String bucketPassword;
	
	@Value(value = "${couchbase.key.type}")
	private String keyType;

	@Override
	protected List<String> getBootstrapHosts() {
		// TODO Auto-generated method stub
		String host[] = hostName.split(",");
		return Arrays.asList(host);
	}

	@Override
	protected String getBucketName() {
		// TODO Auto-generated method stub
		return bucketName;
	}

	@Override
	protected String getBucketPassword() {
		// TODO Auto-generated method stub
		return bucketPassword;
	}

	@Override
	public String typeKey() {
		return keyType;
	}
}
