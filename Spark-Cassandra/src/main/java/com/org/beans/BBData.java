package com.org.beans;

import java.util.UUID;
import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Frozen;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "BBData", caseSensitiveKeyspace = false, caseSensitiveTable = false)
public class BBData {

	@Column(name = "bbluuid")
	private UUID bblUuid;

	@PartitionKey(0)
	@Column(name = "beanclassname")
	private String beanClassName;

	@PartitionKey(1)
	@Column(name = "datehourbucket")
	private String dateHourBucket;

	@Column(name = "fullyprocessed")
	private Boolean fullyProcessed;

	@Column(name = "startblock")
	private Boolean startBlock;

	@ClusteringColumn()
	@Column(name = "ts")
	private String ts;

	@Frozen
	@Column(name = "vars")
	private BBVars vars;

}
