package nl.sogyo.jmsproject;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import java.util.Arrays;
import java.util.List;

public class SparkTester implements Runnable {
	public void run2() {
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
	

	public void run() {
        Logger.getLogger("org").setLevel(Level.WARN);
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkTask");
		JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(10));
		streamingContext.receiverStream(new SparkReceiver(StorageLevel.MEMORY_ONLY())).print();
		streamingContext.start();
		try {
			streamingContext.awaitTermination();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}