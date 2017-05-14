package job3.Spark;

import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import jersey.repackaged.com.google.common.collect.Lists;
import scala.Tuple2;

public class Job3Spark {
	private String pathToFile;

	public Job3Spark(String toPathFile) {
		this.pathToFile = toPathFile;	
	}

	public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println("File path or Output location not found!");
			System.exit(1);
		}
		long start = System.currentTimeMillis();
		Job3Spark job3 = new Job3Spark(args[0]);
		job3.relatedUsers().repartition(1).sortByKey().saveAsTextFile(args[1]);
		long end  = System.currentTimeMillis();
		System.out.println("Time elapsed = " + (end-start)/ 1000 + " s");
	}

	public JavaRDD<String> loadData() {
		SparkConf conf = new SparkConf()
				.setAppName("Job1Spark")
				.setMaster("local[*]");

		@SuppressWarnings("resource")
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> fileLines = sc.textFile(this.pathToFile);
		JavaRDD<String> lines = fileLines.flatMap(line -> Arrays.asList(line.split("\n")).iterator());
		
		return lines;		
	}

	public JavaPairRDD<String,String> relatedUsers() {
		JavaRDD<String> lines = loadData();

		JavaPairRDD<String,String> productUser = lines.filter(line -> Integer.parseInt(line.split("\t")[6]) >= 4)
				.mapToPair(row -> new Tuple2<>(row.split("\t")[1], row.split("\t")[2]));
		JavaPairRDD<String, Tuple2<String, String>> couples = productUser.join(productUser);
		return couples
				.filter(field -> field._2._1.compareTo(field._2._2) < 0)
				.mapToPair(line -> new Tuple2<String,String>(line._2._1 + "-" + line._2._2,line._1))
				.groupByKey()
				.filter(f -> Lists.newArrayList(f._2).size()>=3)
				.mapToPair(f -> new Tuple2<String,String>(f._1,Lists.newArrayList(f._2).toString()));


	}
}
