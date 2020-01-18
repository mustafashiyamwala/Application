package com.org.main;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.Tuple3;

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

		List<String> takeSam = filterRow.takeSample(false, 10);
		System.out.println("Take Sample: " + takeSam);

		JavaRDD<String> sampleRDD = filterRow.sample(true, 0.1);
		System.out.println(sampleRDD.count());

		JavaRDD<String> mapRow = filterRow.map(records -> {
			String[] row = records.split(",");
			return row[2] + "," + row[7] + "," + row[4];
		});

		List<String> top = mapRow.top(10);
		System.out.println("Map Records: " + top);

		JavaRDD<String> mapPartitionsWithIndex = javaRDD
				.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {

					@Override
					public Iterator<String> call(Integer partitionIndex, Iterator<String> partitionData)
							throws Exception {
						// TODO Auto-generated method stub
						System.out.println("Partition Index: " + partitionIndex);

						List<String> list = new ArrayList<>();
						while (partitionData.hasNext()) {
							String records = (String) partitionData.next();
							String[] row = records.split(",");
							list.add(row[2] + "," + row[7] + "," + row[4]);

						}
						return list.iterator();
					}
				}, true);

		System.out.println("Map Partition with Index: " + mapPartitionsWithIndex.first());

		System.out.println("Count: " + javaRDD.count());

		JavaRDD<String> sortRow = javaRDD.sortBy(records -> {
			String[] row = records.split(",");
			return row[0];
		}, true, 1);

		List<String> sample = sortRow.take(10);
		System.out.println("Record: " + sample);

		JavaRDD<String> javaRDD1 = javaSparkContext
				.textFile("file:///C:/Users/Owner/Downloads/1000-Records/1000Records.csv");

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

		JavaPairRDD<String, Integer> foldPairRDD = javaPairRDD.foldByKey(10,
				new Function2<Integer, Integer, Integer>() {

					@Override
					public Integer call(Integer salary1, Integer salary2) throws Exception {
						// TODO Auto-generated method stub
						return salary1 + salary2;
					}
				});

		List<Tuple2<String, Integer>> list2 = foldPairRDD.collect();
		for (Tuple2<String, Integer> tuple2 : list2) {
			System.out.println("Gender: " + tuple2._1 + " Total Salary Fold: " + tuple2._2);
		}

		JavaPairRDD<String, Integer> aggregatePair = javaPairRDD.aggregateByKey(10,
				new Function2<Integer, Integer, Integer>() {

					@Override
					public Integer call(Integer val1, Integer val2) throws Exception {
						// TODO Auto-generated method stub
						return val1 + val2;
					}
				}, new Function2<Integer, Integer, Integer>() {

					@Override
					public Integer call(Integer val1, Integer val2) throws Exception {
						// TODO Auto-generated method stub
						return val1 + val2;
					}
				});

		System.out.println("Aggregate Pair" + aggregatePair.first());

		Tuple2<String, Integer> reduce = javaPairRDD
				.reduce(new Function2<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>>() {

					@Override
					public Tuple2<String, Integer> call(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2)
							throws Exception {
						// TODO Auto-generated method stub
						if (!value1._1.equalsIgnoreCase(value2._1)) {
							
						}
						return new Tuple2<String, Integer>(value1._1, (value1._2 + value2._2));
						
					}
				});

		System.out.println("Reduce: " + reduce);

		Map<String, Long> map = reducePairRDD.countByKey();
		System.out.println("Count By Key: " + map);

		Map<Tuple2<String, Integer>, Long> map1 = reducePairRDD.countByValue();
		System.out.println("Count By Value: " + map1);

		List<Tuple2<String, Integer>> takeOrder = reducePairRDD.takeOrdered(2, new Comparison());
		System.out.println("Take Order: " + takeOrder.get(0));

		JavaPairRDD<String, Tuple3<String, String, String>> javaPairRDD1 = filterRow.mapToPair(row -> {
			String[] records = row.split(",");
			return new Tuple2(records[0], new Tuple3<String, String, String>(records[2], records[7], records[4]));
		});

		JavaPairRDD<String, String> javaPairRDD2 = filterRow.mapToPair(row -> {
			String[] records = row.split(",");
			return new Tuple2(records[0], records[25]);
		});

		JavaPairRDD<String, Tuple2<Tuple3<String, String, String>, String>> joins = javaPairRDD1.join(javaPairRDD2);
		System.out.println("Join: " + joins.first());

		JavaPairRDD<Tuple2<String, Tuple3<String, String, String>>, Tuple2<String, String>> cart = javaPairRDD1
				.cartesian(javaPairRDD2);
		System.out.println("Cartesian: " + cart.take(10));

		JavaPairRDD<String, Tuple2<Iterable<Tuple3<String, String, String>>, Iterable<String>>> coGroup = javaPairRDD1
				.cogroup(javaPairRDD2);
		System.out.println("Co Group" + coGroup.first());

		JavaRDD<String> flat = joins
				.flatMap(new FlatMapFunction<Tuple2<String, Tuple2<Tuple3<String, String, String>, String>>, String>() {

					@Override
					public Iterator<String> call(Tuple2<String, Tuple2<Tuple3<String, String, String>, String>> row)
							throws Exception {
						// TODO Auto-generated method stub
						String empId = row._1;
						Tuple2<Tuple3<String, String, String>, String> records = row._2;

						Tuple3<String, String, String> records1 = records._1;
						String sal = records._2;

						String firstName = records1._1();
						String middleName = records1._2();
						String lastName = records1._3();
						return Arrays.asList(empId + "," + firstName + "," + middleName + "," + lastName + "," + sal)
								.iterator();
					}
				});

		System.out.println(flat.take(5));

		Map<String, String> maps = new HashMap<String, String>();
		maps.put("Mustafa", "E10001");
		maps.put("Raj", "E10002");

		List<Map<String, String>> row = new ArrayList<Map<String, String>>();
		row.add(maps);

		//JavaRDD<Integer> rdd = javaSparkContext.parallelize(Arrays.asList(1, 2, 3));
		
		JavaRDD<Map<String, String>> rdd = javaSparkContext.parallelize(row);
		System.out.println(rdd.collect());

		Broadcast<Map<String, String>> broadcast = javaSparkContext.broadcast(maps);
		Map<String, String> val = broadcast.value();
		System.out.println("Broadcast: " + val);

		// JavaPairRDD<String, Integer> aggregatePairRDD =
		// javaPairRDD.aggregateByKey(zeroValue, seqFunc, combFunc);

		// javaPairRDD.repartition(2);
		// javaRDD.coalesce(1);
		// javaRDD.saveAsTextFile("");

		// reducePairRDD.persist(StorageLevel.MEMORY_AND_DISK_SER_2());
		// reducePairRDD.unpersist();

	}
}

class Comparison implements Comparator<Tuple2<String, Integer>>, Serializable {

	@Override
	public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
		// TODO Auto-generated method stub
		return o1._1().compareTo(o2._1());
	}

}