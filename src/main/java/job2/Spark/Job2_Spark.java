package job2.Spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import com.clearspring.analytics.util.Lists;

import scala.Tuple2;


public class Job2_Spark {
	
	private String pathToFile;

	public Job2_Spark(String path) {
		this.pathToFile = path; 		
	}
	
	public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("File path not found!");
			System.exit(1);
		}
		Job2_Spark topTenProducts = new Job2_Spark(args[0]);
		JavaPairRDD<String, Iterable<ProductSerializable>> output = topTenProducts.userVotes().coalesce(1);
		output.map(x -> x._1 + "\t" + x._2).saveAsTextFile(args[1]);
	}
	
	public JavaRDD<String> loadData() {
		SparkConf conf = new SparkConf()
				.setAppName("Job2_Spark");

		@SuppressWarnings("resource")
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> fileLines = sc.textFile(this.pathToFile);
		JavaRDD<String> lines = fileLines.flatMap(line -> Arrays.asList(line.split("\n")).iterator());

		return lines;

	}
	
	public JavaPairRDD<String, Iterable<ProductSerializable>> userVotes() {
		JavaRDD<String> lines = loadData();
		@SuppressWarnings("unchecked")
		JavaPairRDD<String,ProductSerializable> usersVotes = lines.flatMapToPair( line ->{
			@SuppressWarnings("rawtypes")
			List results = new ArrayList();
			if (!line.startsWith("id") || line != null || !line.equals("")) {
				List<String> splitLine = Arrays.asList(line.split("\t"));
				String productId = splitLine.get(1);
				String userId = splitLine.get(2);
				Double score = Double.parseDouble(splitLine.get(6));
				ProductSerializable product = new ProductSerializable(productId, score);
				Tuple2<String,ProductSerializable> tuple = new Tuple2<> (userId,product);
				results.add(tuple);
			}
			return results.iterator();			
		});
		return usersVotes.groupByKey().sortByKey().mapValues(values -> {
			List<ProductSerializable> list = Lists.newArrayList(values);
			Collections.sort(list);
			Collections.reverse(list);
			if (list.size() >= 10) {
				return list.subList(0, 10);
			}
			return list;
		});
	}

}
