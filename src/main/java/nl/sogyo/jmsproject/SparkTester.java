package nl.sogyo.jmsproject;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class SparkTester implements Runnable {
	public void run() {
		SparkConf conf = new SparkConf()
			.setAppName("SparkTest")
			.setMaster("local[*]");
		JavaSparkContext context = new JavaSparkContext(conf);

		List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
		JavaRDD<Integer> distData = context.parallelize(data);
		JavaRDD<Integer> squared = distData.map(x -> x*x);
		List<Integer> data2 = squared.collect();
		for (Integer integer : data2){
			System.out.println("Squared:" + integer);
		}
	}
	
	/*
	public void run2() {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkTask");
		JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(10));
		streamingContext.receiverStream(<<< super(StorageLevel.MEMORY_ONLY()) extends Receiver<Status> >>>)
			.foreachRDD(
				rdd -> rdd.coalesce(10)
					.foreach(message -> message.getText()));
		streamingContext.start();
		streamingContext.awaitTermination();
	}
	*/
}