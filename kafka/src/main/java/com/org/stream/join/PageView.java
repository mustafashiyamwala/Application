package com.org.stream.join;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class PageView {
	private Integer userID;
	private String page;
}
