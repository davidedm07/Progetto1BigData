package job1.Spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.clearspring.analytics.util.Lists;

import scala.Tuple2;

public class Job1Spark implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String pathToFile;

	public Job1Spark(String path) {
		this.pathToFile = path; 		
	}

	public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println("File path or Output location not found!");
			System.exit(1);
		}
		Job1Spark top5products = new Job1Spark(args[0]);
		JavaPairRDD<String, Iterable<Tuple2<String,Double>>> result = top5products.top5products().sortByKey();
		result.saveAsTextFile(args[1]);

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

	public JavaPairRDD<String,Double> averageScore(JavaRDD<String> lines) {
		@SuppressWarnings("unchecked")
		JavaPairRDD<String,Tuple2<Double,Double>> singleProductScoreCont = lines.flatMapToPair( line ->{
			@SuppressWarnings("rawtypes")
			List results = new ArrayList();
			if (!line.startsWith("id") || line != null || !line.equals("")) {
				List<String> splitLine = Arrays.asList(line.split("\t"));
				String productId = splitLine.get(1);
				Double score = Double.parseDouble(splitLine.get(6));
				Date time =new Date(Long.parseLong(splitLine.get(7))*1000);
				Calendar cal = Calendar.getInstance();
				cal.setTime(time);
				int year = cal.get(Calendar.YEAR);
				int month = cal.get(Calendar.MONTH) + 1;
				String sMonth = "";
				if (month<10)
					sMonth = "0" + month;
				else 
					sMonth = "" + month;
				String key = "" + year + "-" + sMonth + "-" + productId;
				Tuple2<String,Tuple2<Double,Double>> tuple = new Tuple2<> (key,new Tuple2<>(score,1.0));
				results.add(tuple);
			}
			return results.iterator();			
		});
		// Tuples with (sum of score, cont), average later computed as sum of scores/cont
		JavaPairRDD <String,Tuple2<Double,Double>> keyScoreCont = singleProductScoreCont.aggregateByKey(new Tuple2<>(0d,0d), 
				(a,b) -> new Tuple2<Double,Double>(a._1+b._1,a._2 +b._2),
				(a,b)-> new Tuple2<Double,Double>(a._1+b._1,a._2 +b._2));

		JavaPairRDD<String,Double> averageScore = keyScoreCont.mapValues(a -> {return a._1()/a._2();});
		return averageScore;
	}

	@SuppressWarnings("unchecked")
	public JavaPairRDD<String,Iterable<Tuple2<String,Double>>> top5products() {
		JavaRDD<String> lines = loadData().cache();
		JavaPairRDD<String,Tuple2<String,Double>> top5 = averageScore(lines).flatMapToPair(line -> {
			List<String> splitLine = Arrays.asList(line._1.split("-"));
			@SuppressWarnings("rawtypes")
			List results = new ArrayList();
			String newKey = splitLine.get(0) + "-" + splitLine.get(1);
			Tuple2<String,Double> productIdAverageScore = new Tuple2<>(splitLine.get(2),line._2);
			Tuple2<String,Tuple2<String,Double>> result = new Tuple2<>(newKey,productIdAverageScore);
			results.add(result);
			return results.iterator();

		});
		return top5.groupByKey().mapValues(a->  {
			ArrayList<Tuple2<String,Double>> l = (ArrayList<Tuple2<String, Double>>) Lists.newArrayList(a);
			Collections.sort(l, new Comparator<Tuple2<String,Double>>() {
				@Override
				public int compare(Tuple2<String, Double> o1, Tuple2<String, Double> o2) {
					return o1._2().compareTo(o2._2());
				}
			});
			Collections.reverse(l);
			int cont = 0;
			List<Tuple2<String,Double>> top5productsForAverage = new LinkedList<>();
			for (Tuple2<String,Double> t: l) {
				if (cont >=5)
					break;
				cont ++;
				top5productsForAverage.add(t);	 
			};
			return top5productsForAverage;
		});
	}

}