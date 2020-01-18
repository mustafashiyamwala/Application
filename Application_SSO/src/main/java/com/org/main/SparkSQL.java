package com.org.main;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

import java.util.Arrays;
import java.util.List;

public class SparkSQL {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("Spark-SQL").getOrCreate();
		Dataset<Row> dataFrame = sparkSession.read().format("csv").option("header", "true").option("sep", ",")
				.load("file:///C:/Users/Owner/Downloads/1000-Records/1000Records.csv");

		dataFrame.printSchema();
		dataFrame.show();

		dataFrame.select(col("First Name"), col("Age in Company (Years)").plus(1).alias("Experience"))
				.filter(col("Year of Joining").gt("2010").and(col("Year of Joining").lt("2015"))).show();

		dataFrame
				.select(col("First Name").alias("FirstName"), col("Age in Company (Years)").plus(1).alias("Experience"),
						col("Gender").alias("Gender"))
				.filter(col("Year of Joining").gt("2010").and(col("Year of Joining").lt("2015"))).write()
				.mode(SaveMode.Ignore).format("parquet")
				.save("file:///C:/Users/Owner/Downloads/1000-Records/employee.parquet");

		dataFrame.groupBy(col("Gender")).count().show();

		dataFrame.createOrReplaceTempView("employee");
		sparkSession.sql("select * from employee").show();

		dataFrame.createOrReplaceGlobalTempView("employee_records");
		sparkSession.sql("select * from global_temp.employee_records").show();

		sparkSession.newSession().sql("select * from global_temp.employee_records").show();

		Dataset<Row> parquet = sparkSession
				.sql("SELECT * FROM parquet.`file:///C:/Users/Owner/Downloads/1000-Records/employee.parquet`");
		parquet.show();

		parquet.write().bucketBy(5, "FirstName").sortBy("Experience").partitionBy("Gender").saveAsTable("records");

		List<String> jsonData = Arrays
				.asList("{\"name\":\"Yin\",\"address\":{\"city\":\"Columbus\",\"state\":\"Ohio\"}}");
		Dataset<String> dataset = sparkSession.createDataset(jsonData, Encoders.STRING());
		Dataset<Row> dataframe1 = dataset.toDF();
		dataframe1.show();

		Dataset<Row> sql = sparkSession.read().format("jdbc").option("url", "jdbc:mysql://localhost:3306/")
				.option("dbtable", "testDB.employee").option("user", "root").option("password", "YaAli@52").load();
		sql.show();
	}

}
