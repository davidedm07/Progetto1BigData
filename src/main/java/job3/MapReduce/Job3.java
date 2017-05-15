package job3.MapReduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Job3 {

	public static void main(String[] args) throws Exception {
		long start = System.currentTimeMillis();
		Job job3Part1 = new Job(new Configuration(),"Job3Part1");
		job3Part1.setJarByClass(Job3.class);
		job3Part1.setMapperClass(Job3Mapper1.class);
		job3Part1.setReducerClass(Job3Reducer1.class);
		FileInputFormat.addInputPath(job3Part1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job3Part1, new Path(args[1]));
		job3Part1.setMapOutputKeyClass(Text.class);
		job3Part1.setMapOutputValueClass(Text.class);
		job3Part1.setOutputKeyClass(Text.class);
		job3Part1.setOutputValueClass(Text.class);
		job3Part1.waitForCompletion(true);
		Job job3Part2 = new Job(new Configuration(),"Job3Part2");
		job3Part2.setJarByClass(Job3.class);
		job3Part2.setMapperClass(Job3Mapper2.class);
		job3Part2.setReducerClass(Job3Reducer2.class);
		FileInputFormat.addInputPath(job3Part2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job3Part2, new Path(args[2]));
		job3Part2.setMapOutputKeyClass(Text.class);
		job3Part2.setMapOutputValueClass(Text.class);
		job3Part2.setOutputKeyClass(Text.class);
		job3Part2.setOutputValueClass(Text.class);
		job3Part2.waitForCompletion(true);
		long end  = System.currentTimeMillis();
		System.out.println("Time elapsed = " + (end-start)/ 1000 + " s");
	}

}
