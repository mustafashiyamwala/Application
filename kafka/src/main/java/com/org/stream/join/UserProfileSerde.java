package com.org.stream.join;

import com.org.stream.serde.JsonDeserializer;
import com.org.stream.serde.JsonSerializer;
import com.org.stream.serde.WrapperSerde;

public class UserProfileSerde extends WrapperSerde<UserProfile> {

	public UserProfileSerde() {
		super(new JsonSerializer<UserProfile>(), new JsonDeserializer<UserProfile>(UserProfile.class));
		// TODO Auto-generated constructor stub
	}
}
