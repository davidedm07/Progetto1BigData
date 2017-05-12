package job1.MapReduce.basic;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;



public class ProductWritable implements WritableComparable<ProductWritable> {

	private Text productID= new Text();
	private DoubleWritable averageScore = new DoubleWritable();

	public ProductWritable() {
		
	}
	public ProductWritable(Text productId,DoubleWritable avgScore) {
		this.productID = productId;
		this.averageScore = avgScore;

	}

	public void readFields(DataInput in) throws IOException {
		this.productID.readFields(in);
		this.averageScore.readFields(in);

	}

	public void write(DataOutput out) throws IOException {
		this.productID.write(out);
		this.averageScore.write(out);

	}

	public Text getProductID() {
		return productID;
	}


	public void setProductID(Text productID) {
		this.productID = productID;
	}


	public DoubleWritable getAverageScore() {
		return averageScore;
	}


	public void setAverageScore(DoubleWritable score) {
		this.averageScore = score;
	}


	@Override
	public String toString() {
		return "Product Id: " + this.productID.toString() + "Average Score: "  + this.averageScore.toString();
	}

	@Override 
	public boolean equals(Object o) {
		ProductWritable p = (ProductWritable) o;
		if (this.productID.toString().equals(p.getProductID().toString()))
			return true;
		return false;
	}


	public int compareTo(ProductWritable o) {
		// TODO Auto-generated method stub
		if (this.averageScore.get() > o.getAverageScore().get())
			return 1;
		else if (this.averageScore.get() < o.getAverageScore().get())
			return -1;
		else 
			return 0;
	}
	
	@Override
	public int hashCode() {
		return this.productID.hashCode() + this.averageScore.hashCode();
	}
	
	
}