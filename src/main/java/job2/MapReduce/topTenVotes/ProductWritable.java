package job2.MapReduce.topTenVotes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class ProductWritable implements WritableComparable<ProductWritable>{

	private Text productId = new Text();
	private DoubleWritable score = new DoubleWritable();
	
	public ProductWritable() {}
	
	public ProductWritable(Text productId, DoubleWritable score) {
		this.productId = productId;
		this.score = score;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.productId.readFields(in);
		this.score.readFields(in);
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.productId.write(out);
		this.score.write(out);
	}
	
	public Text getProductId() {
		return productId;
	}
	
	public void setProductId(Text productId) {
		this.productId = productId;
	}
	
	public DoubleWritable getScore() {
		return score;
	}
	
	public void setScore(DoubleWritable score) {
		this.score = score;
	}

	@Override
	public String toString() {
		return "Product Id: " + this.productId.toString() + " Score: "  + this.score.toString();
	}
	
	@Override 
	public boolean equals(Object o) {
		ProductWritable p = (ProductWritable) o;
		if (this.productId.toString().equals(p.getProductId().toString()))
			return true;
		return false;
	}
	
	@Override
	public int compareTo(ProductWritable o) {
		Double d = this.score.get() - o.getScore().get();
		return d.intValue();
	}
	
	@Override
	public int hashCode() {
		return this.productId.hashCode() + this.score.hashCode();
	}
	
}
