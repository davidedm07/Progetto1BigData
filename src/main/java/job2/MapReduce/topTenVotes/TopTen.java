package job2.MapReduce.topTenVotes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopTen {

	public static void main(String[] args) throws Exception{
		long startTime = System.currentTimeMillis();
		Job job = new Job(new Configuration(), "Job1");
		job.setJarByClass(TopTen.class);
		job.setMapperClass(TopTenMapper.class);
		job.setReducerClass(TopTenReducer.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ProductWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		job.waitForCompletion(true);
		long endTime   = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		System.out.println("Execution has taken: " + String.valueOf(totalTime/1000));
	}

}
