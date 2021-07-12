package com.org.bundle;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;

import scala.Tuple1;
import scala.Tuple2;
import scala.collection.Seq;

public class Testing {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		SparkConf conf = new SparkConf().setAppName("m").setMaster("local[*]");
		JavaSparkContext context = new JavaSparkContext(conf);
		SparkSession sparkSession = new SparkSession(context.sc());

		/*
		 * JavaRDD<String> rdd = context.parallelize(Arrays.asList("Visa", "Diners",
		 * "Amex")); Dataset<Row> row = sparkSession.read().option("header",
		 * true).csv("src/main/resources/SalesJan2009.csv");
		 * row.select("Payment_Type").distinct().show(); String[] seq =
		 * rdd.collect().toArray(new String[0]);
		 * row.write().partitionBy(seq).csv("src/main/resources/output");
		 */

		JavaRDD<String> rdd = context.textFile("src/main/resources/SalesJan2009.csv");
		JavaPairRDD<String, String> rdd21 = rdd.mapToPair(a -> {
			String[] b = a.split(",");
			return new Tuple2<String, String>(b[3], b[2]);
		});
		JavaPairRDD<String, Iterable<String>> rdd31 = rdd21.groupByKey();

		rdd31.collect().forEach(t -> {List<String> target = new ArrayList<>();
		t._2.forEach(target::add);
		System.out.println("========>" +context.parallelize(target).collect());});

		//System.out.println("========>" + rdd31.collect());

		/*
		 * JavaRDD<Integer> rdd = context.parallelize(Arrays.asList(1, 2, 3, 1, 2, 1));
		 * 
		 * JavaPairRDD<Integer, Integer> pairrdd = rdd.mapToPair(a -> new
		 * Tuple2<Integer, Integer>(a, 1)); JavaPairRDD<Integer, Iterable<Integer>>
		 * grouprdd = pairrdd.groupByKey();
		 * 
		 * 
		 * grouprdd.wr
		 * 
		 * 
		 * grouprdd.foreachPartition(new
		 * VoidFunction<Tuple2<Integer,Iterable<Integer>>>() {
		 * 
		 * @Override public void call(Tuple2<Integer, Iterable<Integer>> t) throws
		 * Exception { // TODO Auto-generated method stub List<Integer> target = new
		 * ArrayList<>();; System.out.println("========>" +t._1+" "+
		 * context.parallelize(t._2.iterator().forEachRemaining(target::add)).collect())
		 * ; } });
		 * 
		 * 
		 * //a -> {System.out.println("========>" a._1+" "+
		 * context.parallelize(a._2).collect());
		 * 
		 * JavaRDD<Integer> flatrdd = grouprdd.map(a -> a._2).flatMap(b->b.iterator());
		 * System.out.println("========>" +flatrdd.collect());
		 */

	}

}
