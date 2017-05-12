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
		//JavaPairRDD<String, Iterable<Tuple2<String,Double>>> result = topTenProducts.top5products().sortByKey();
		//result.saveAsTextFile("/home/davide/Scrivania/risultato.txt");
		//JavaRDD<String> output = topTenProducts.loadData();
		output.map(x -> x._1 + "," + x._2).saveAsTextFile(args[1]);
	}
	
	public JavaRDD<String> loadData() {
		SparkConf conf = new SparkConf()
				.setAppName("Job2_Spark");
		//.setMaster("local[*]"); // here local mode. And * means you will use as much as you have cores.

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
			if (list.size() >= 5) {
				return list.subList(0, 5);
			}
			return list;
		});
		// Tuples with (sum of score, cont), average later computed as sum of scores/cont
		/*JavaPairRDD <String,ArrayList<ProductWritable>> keyScoreCont = usersVotes.aggregateByKey(new ArrayList<>(0d,0d), 
				(a,b) -> new Tuple2<Double,Double>(a._1+b._1,a._2 +b._2),
				(a,b)-> new Tuple2<Double,Double>(a._1+b._1,a._2 +b._2));

		JavaPairRDD<String,Double> averageScore = keyScoreCont.mapValues(a -> {return a._1()/a._2();});
		return averageScore;*/
	}
	
	/*@SuppressWarnings("unchecked")
	public JavaPairRDD<String,Iterable<Tuple2<String,Double>>> topTenproducts() {
		JavaPairRDD<String,Tuple2<String,Double>> topTen = userVotes().flatMapToPair(line -> {
			List<String> splitLine = Arrays.asList(line._1.split("-"));
			@SuppressWarnings("rawtypes")
			List results = new ArrayList();
			String newKey = splitLine.get(0) + "-" + splitLine.get(1);
			Tuple2<String,ProductWritable> productIdAverageScore = new Tuple2<>(splitLine.get(2),line._2);
			Tuple2<String,Tuple2<String,ProductWritable>> result = new Tuple2<>(newKey,productIdAverageScore);
			results.add(result);
			return results.iterator();

		});
		return topTen.groupByKey().mapValues(a->  {
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
	}*/
	
}
