package job2.Spark;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import scala.Serializable;



public class ProductSerializable implements Serializable, Comparable<ProductSerializable> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String productID;
	private Double averageScore;

	public ProductSerializable() {
		
	}
	public ProductSerializable(String productId,Double avgScore) {
		this.productID = productId;
		this.averageScore = avgScore;

	}

	public String getProductID() {
		return productID;
	}


	public void setProductID(String productID) {
		this.productID = productID;
	}


	public Double getAverageScore() {
		return averageScore;
	}


	public void setAverageScore(Double score) {
		this.averageScore = score;
	}


	@Override
	public String toString() {
		return "Product Id: " + this.productID.toString() + " Score: "  + this.averageScore.toString();
	}

	@Override 
	public boolean equals(Object o) {
		ProductSerializable p = (ProductSerializable) o;
		if (this.productID.toString().equals(p.getProductID().toString()))
			return true;
		return false;
	}

	@Override
	public int compareTo(ProductSerializable o) {
		// TODO Auto-generated method stub
		if (this.averageScore.doubleValue() > o.getAverageScore().doubleValue())
			return 1;
		else if (this.averageScore.doubleValue() < o.getAverageScore().doubleValue())
			return -1;
		else 
			return 0;
	}
	
	@Override
	public int hashCode() {
		return this.productID.hashCode() + this.averageScore.hashCode();
	}
	
}