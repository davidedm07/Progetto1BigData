package job3.MapReduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job3Reducer1 extends  Reducer<Text,Text,Text,Text> {

	@Override
	public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
		
		Set<String> distinctUsers = new HashSet<>();
		for (Text value : values)
			distinctUsers.add(value.toString()); 
		
		ArrayList<String> usersPerProduct = new ArrayList<>(distinctUsers);
		for(int i = 0; i< usersPerProduct .size(); i++) {
			// get the single userID
			String userK = usersPerProduct.get(i);
			//create all the possible couples for that userID related to the product passed as key to the reducer
			for (int j=i+1; j< usersPerProduct.size();j++) {
				Text coupleUsers = new Text(userK+ "-" + usersPerProduct.get(j));
				context.write(coupleUsers, key);			
			}
		}

	}

}
