package com.org.stream.join;

import com.org.stream.serde.JsonDeserializer;
import com.org.stream.serde.JsonSerializer;
import com.org.stream.serde.WrapperSerde;

public class UserActivitySerde extends WrapperSerde<UserActivity> {

	public UserActivitySerde() {
		super(new JsonSerializer<UserActivity>(), new JsonDeserializer<UserActivity>(UserActivity.class));
		// TODO Auto-generated constructor stub
	}

}
