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
		
				// 1. textfile --> string dataset return
		Dataset<String> stringDataset = sparkSession.read()
				.textFile("file:///C:/Users/Owner/Downloads/SalesJan2009.csv");

		String firstRow = stringDataset.first();
		Dataset<String> stringFilterDataset = stringDataset.filter(row -> !row.equals(firstRow));

		// convert dataset string to sale type
		Encoder<Sales> encoderSales = Encoders.bean(Sales.class);

		// 2. unable to map flat file? check with json
		// Dataset<Sales> salesDataset = stringFilterDataset.as(encoderSales);

		Dataset<Sales> salesDatasetFlatFile = stringFilterDataset.map(new MapFunction<String, Sales>() {

			@Override
			public Sales call(String row) throws Exception {
				// TODO Auto-generated method stub

				Sales sales = new Sales();
				String column[] = row.split(",");

				DateFormat format = new SimpleDateFormat("dd/MM/yyyy hh:mm");
				Timestamp timeStamp = new Timestamp(format.parse(column[0]).getTime());

				sales.setTransactionDate(timeStamp);
				sales.setProductCode(column[1]);
				sales.setPrice(Double.valueOf(column[2]));
				sales.setCardName(column[3]);
				sales.setName(column[4]);
				sales.setCity(column[5].trim());
				sales.setState(column[6]);
				sales.setCountry(column[7]);

				return sales;
			}
		}, encoderSales);

		// convert ds to df
		Dataset<Row> dsConvertDf = salesDatasetFlatFile.toDF();

		// convert ds to RDD
		JavaRDD<Sales> javaRDDSales = salesDatasetFlatFile.toJavaRDD();

		Dataset<Row> saleDataFrame = sparkSession.read().format("csv").option("header", "true").option("sep", ",")
				.load("file:///C:/Users/Owner/Downloads/SalesJan2009.csv");

		// convert df to ds
		Dataset<Sales> saleDataFrameMap = saleDataFrame.map(new MapFunction<Row, Sales>() {

			@Override
			public Sales call(Row row) throws Exception {
				// TODO Auto-generated method stub
				Sales sales = new Sales();

				DateFormat format = new SimpleDateFormat("dd/MM/yyyy hh:mm");
				Timestamp timeStamp = new Timestamp(format.parse(row.getAs("Transaction_date")).getTime());

				sales.setTransactionDate(timeStamp);
				sales.setProductCode(row.getAs("Product"));
				sales.setPrice(Double.valueOf(Double.valueOf(row.getAs("Price"))));
				sales.setCardName(row.getAs("Payment_Type"));
				sales.setName(row.getAs("Name"));
				sales.setCity(row.getAs("City"));
				sales.setState(row.getAs("State"));
				sales.setCountry(row.getAs("Country"));

				return sales;
			}
		}, encoderSales);

		// saleDataFrameMap.show();

		// convert string ds to string rdd
		JavaRDD<String> rddString = sparkSession.read().textFile("file:///C:/Users/Owner/Downloads/SalesJan2009.csv")
				.javaRDD();

		String firstRow1 = rddString.first();
		JavaRDD<String> stringFilterRdd = rddString.filter(row -> !row.equals(firstRow1));

		// rdd of type sale
		JavaRDD<Sales> rddSales = stringFilterRdd.map(new Function<String, Sales>() {

			@Override
			public Sales call(String row) throws Exception {
				// TODO Auto-generated method stub

				Sales sales = new Sales();
				String column[] = row.split(",");

				DateFormat format = new SimpleDateFormat("dd/MM/yyyy hh:mm");
				Timestamp timeStamp = new Timestamp(format.parse(column[0]).getTime());

				sales.setTransactionDate(timeStamp);
				sales.setProductCode(column[1]);
				sales.setPrice(Double.valueOf(column[2]));
				sales.setCardName(column[3]);
				sales.setName(column[4]);
				sales.setCity(column[5].trim());
				sales.setState(column[6]);
				sales.setCountry(column[7]);

				return sales;
			}
		});

		// convert rdd to ds
		Dataset<Sales> saleRddDs = sparkSession.createDataset(rddSales.rdd(), encoderSales);
		// saleRddDs.show();

		Dataset<Row> saleRddDf = sparkSession.createDataFrame(rddSales, Sales.class);
		// saleRddDf.show();

		// when flat file having no header for df no use of infer schema
		String schemaString = "transactionDate productCode price cardName name city state country";

		String columns[] = schemaString.split(" ");

		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField(columns[0], DataTypes.TimestampType, false));
		fields.add(DataTypes.createStructField(columns[1], DataTypes.StringType, false));
		fields.add(DataTypes.createStructField(columns[2], DataTypes.DoubleType, false));
		fields.add(DataTypes.createStructField(columns[3], DataTypes.StringType, false));
		fields.add(DataTypes.createStructField(columns[4], DataTypes.StringType, false));
		fields.add(DataTypes.createStructField(columns[5], DataTypes.StringType, false));
		fields.add(DataTypes.createStructField(columns[6], DataTypes.StringType, false));
		fields.add(DataTypes.createStructField(columns[7], DataTypes.StringType, false));

		StructType structType = DataTypes.createStructType(fields);

		// 3. differnce between javardd and rdd
		RDD<Row> rddRow = stringFilterRdd.map(new Function<String, Row>() {

			@Override
			public Row call(String record) throws Exception {
				// TODO Auto-generated method stub
				String column[] = record.split(",");

				DateFormat format = new SimpleDateFormat("dd/MM/yyyy hh:mm");
				Timestamp timeStamp = new Timestamp(format.parse(column[0]).getTime());

				return RowFactory.create(timeStamp, column[1], Double.valueOf(column[2]), column[3], column[4],
						column[5].trim(), column[6], column[7]);
			}
		}).rdd();

		Dataset<Row> dfRow = sparkSession.createDataFrame(rddRow, structType);
		// dfRow.show();

		// by default desc
		WindowSpec windowSpec = Window.partitionBy(col("cardName"), col("country")).orderBy(col("country").asc(),
				col("cardName").asc());

		// all column defined in windowing function need to be presnt in select
		// condition
		dfRow.select(col("price"), col("cardName"), col("country"))
				.withColumn("Total", sum(col("price")).over(windowSpec)).drop(col("price")).distinct().show();

	
	}

}
