/*
 * CS 61C Fall 2013 Project 1
 *
 * DoublePair.java is a class which stores two doubles and 
 * implements the Writable interface. It can be used as a 
 * custom value for Hadoop. To use this as a key, you can
 * choose to implement the WritableComparable interface,
 * although that is not necessary for credit.
 */

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class DoublePair implements Writable {
    // Declare any variables here
	double d1;		// Goes into Mapper 
	double d2;

    /**
     * Constructs a DoublePair with both doubles set to zero.
     */
    public DoublePair() {
        // YOUR CODE HERE
		d1 = 0;
		d2 = 0;
    }

    /**
     * Constructs a DoublePair containing double1 and double2.
     */ 
    public DoublePair(double double1, double double2) {
        // YOUR CODE HERE
		d1 = double1;
		d2 = double2;
    }

    /**
     * Returns the value of the first double.
     */
    public double getDouble1() {
        // YOUR CODE HERE
        return d1;
    }

    /**
     * Returns the value of the second double.
     */
    public double getDouble2() {
        // YOUR CODE HERE
        return d2;
    }

    /**
     * Sets the first double to val.
     */
    public void setDouble1(double val) {
        // YOUR CODE HERE
		d1 = val;
    }

    /**
     * Sets the second double to val.
     */
    public void setDouble2(double val) {
        // YOUR CODE HERE
		d2 = val;
    }

    /**
     * write() is required for implementing Writable.
     */
    public void write(DataOutput out) throws IOException {
        // YOUR CODE HERE
		out.writeDouble(d1);
		out.writeDouble(d2);
    }

    /**
     * readFields() is required for implementing Writable.
     */
    public void readFields(DataInput in) throws IOException {
        // YOUR CODE HERE
		d1 = in.readDouble();
		d2 = in.readDouble();
    }
	
	public boolean equals(DoublePair o) {
		return (getDouble1() == o.getDouble1()) && (getDouble2() == o.getDouble2());
	}
	
	public int hashCode() {
		int hash = 17;
		hash = hash * 31 + new Double(d1).hashCode();
		hash = hash * 31 + new Double(d2).hashCode();
		return hash;
	}
	public int compareTo(DoublePair o) {
		double temp1 = d1;
		double temp2 = o.d1;
		if (temp1 < temp2) {
			return -1;
		}
		else if (temp1 > temp2) {
			return 1; 
		}
		else {
			return 0;
		}
	}
}			
