package job1.MapReduce.SingleMR;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import job1.MapReduce.basic.ProductWritable;


public class Job1Reducer extends  Reducer<Text, IntWritable, Text, Text> {

	private HashMap<String,ArrayList<ProductWritable>> monthYearProducts = 
			new HashMap<String,ArrayList<ProductWritable>>();

	@Override
	public void reduce(Text key,Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException {
		double sum = 0.0;
		double avg;
		int cont = 0;
		for (IntWritable i : values) { 
			sum += i.get();
			cont ++;
		}
		avg = sum/cont;
		String[] splitKey = key.toString().split("-");
		String newKey = splitKey[0] + "-" + splitKey[1];
		String productID = splitKey[2];
		ProductWritable p = new ProductWritable(new Text(productID),new DoubleWritable(avg));
		if (this.monthYearProducts.containsKey(newKey))
			this.monthYearProducts.get(newKey).add(p);
		else {
			ArrayList<ProductWritable> list = new ArrayList<ProductWritable>();
			list.add(p);
			this.monthYearProducts.put(newKey, list);
		}

	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		ArrayList<String> sortedKeys = new ArrayList<String>(this.monthYearProducts.keySet());
		Collections.sort(sortedKeys);
		for (String k : sortedKeys) {
			Collections.sort(this.monthYearProducts.get(k));
			Collections.reverse(this.monthYearProducts.get(k));
			int cont = 0;
			String top5Products = "";
			for (ProductWritable p : this.monthYearProducts.get(k)) {
				if (cont++ >= 5)
					break;
				top5Products += p.getProductID().toString() + "\t" + p.getAverageScore().toString() + "\t ";

			}
			context.write(new Text(k),new Text(top5Products));

		}
	}

}