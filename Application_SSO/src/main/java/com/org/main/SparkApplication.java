package com.org.main;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

public class SparkApplication {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("Spark-App").setMaster("local[*]");
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

		JavaRDD<String> javaRDD = javaSparkContext
				.textFile("file:///C:/Users/Owner/Downloads/1000-Records/1000Records.csv");

		String firstRow = javaRDD.first();
		System.out.println("First Row:" + firstRow);

		List<String> list = javaRDD.collect();
		System.out.println("List of Records: " + list.get(0));

		JavaRDD<String> filterRow = javaRDD.filter(row -> !firstRow.equalsIgnoreCase(row));
		List<String> limit = filterRow.take(1);
		System.out.println("Filter Records: " + limit.get(0));

		JavaRDD<String> mapRow = filterRow.map(records -> {
			String[] row = records.split(",");
			return row[2] + "," + row[7] + "," + row[4];
		});

		List<String> top = mapRow.top(10);
		System.out.println("Map Records: " + top);

		System.out.println("Count: " + javaRDD.count());

		JavaRDD<String> sortRow = javaRDD.sortBy(records -> {
			String[] row = records.split(",");
			return row[0];
		}, true, 1);

		List<String> sample = sortRow.take(10);
		System.out.println("Record: " + sample);

		JavaRDD<String> javaRDD1 = javaSparkContext
				.textFile("file:///C:/Users/Owner/Downloads/1000-Records/1000Records.csv");

		String firstRow1 = javaRDD1.first();
		JavaRDD<String> filterRow1 = javaRDD1.filter(row -> !firstRow1.equalsIgnoreCase(row));

		JavaRDD<String> unionRow = javaRDD.union(javaRDD1);
		System.out.println("Count: " + unionRow.count());

		JavaRDD<String> distinctRow = unionRow.distinct();
		System.out.println("Count: " + distinctRow.count());

		JavaRDD<String> intersectRow = javaRDD.intersection(javaRDD1);
		System.out.println("Count: " + intersectRow.count());

		JavaRDD<String> uncommonRow = javaRDD.subtract(javaRDD1);
		System.out.println("Count: " + uncommonRow.count());

		JavaPairRDD<String, Integer> javaPairRDD = filterRow.mapToPair(row -> {
			String[] records = row.split(",");

			return new Tuple2(records[5], Integer.parseInt(records[25]));
		});

		JavaPairRDD<String, Iterable<Integer>> groupPairRDD = javaPairRDD.groupByKey();
		groupPairRDD.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {

			@Override
			public void call(Tuple2<String, Iterable<Integer>> t) throws Exception {
				// TODO Auto-generated method stub
				String gender = t._1;
				Iterable<Integer> salary = t._2;

				Integer totalSalary = 0;
				for (Integer sal : salary) {
					totalSalary += sal;
				}
				System.out.println("Gender: " + gender + " Total Salary: " + totalSalary);
			}
		});

		JavaPairRDD<String, Integer> reducePairRDD = javaPairRDD
				.reduceByKey(new Function2<Integer, Integer, Integer>() {

					@Override
					public Integer call(Integer salary1, Integer salary2) throws Exception {
						// TODO Auto-generated method stub
						return salary1 + salary2;
					}
				});

		List<Tuple2<String, Integer>> list1 = reducePairRDD.collect();
		for (Tuple2<String, Integer> tuple2 : list1) {
			System.out.println("Gender: " + tuple2._1 + " Total Salary: " + tuple2._2);
		}

		Map<String, Long> map = reducePairRDD.countByKey();
		System.out.println("Count By Key: " + map);

		reducePairRDD.takeOrdered(2, new Comparator<Tuple2<String, Integer>>() {

			@Override
			public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
				// TODO Auto-generated method stub
				return o1._1.compareTo(o2._1);

			}
		});

		// JavaPairRDD<String, Integer> aggregatePairRDD =
		// javaPairRDD.aggregateByKey(zeroValue, seqFunc, combFunc);

		// javaPairRDD.repartition(2);
		// javaRDD.coalesce(1);
		// javaRDD.saveAsTextFile("");

		// reducePairRDD.persist(StorageLevel.MEMORY_AND_DISK_SER_2());
		// reducePairRDD.unpersist();

	}
}
