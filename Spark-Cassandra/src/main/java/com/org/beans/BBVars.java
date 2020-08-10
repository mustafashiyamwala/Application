package com.org.beans;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@UDT(name = "BBVars", caseSensitiveKeyspace = false, caseSensitiveType = false)
public class BBVars {

	@Field(name = "mt_aiflo")
	private Double mt_aiflo;

	@Field(name = "mt_aitmp")
	private Double mt_aitmp;

	@Field(name = "mt_aiden")
	private Double mt_aiden;

}
