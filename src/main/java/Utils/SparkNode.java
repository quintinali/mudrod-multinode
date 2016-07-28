package Utils;

import java.io.Serializable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkNode implements Serializable  {
	
	 /*  private static JavaSparkContext static_sc = null;
	   public SparkNode() {
	      // Exists only to defeat instantiation.
	   }
	   public static JavaSparkContext getSC() {
	      if(static_sc == null) {
	    	  SparkConf conf = new SparkConf().setAppName("Testing").setMaster("local");
	    	  static_sc = new JavaSparkContext(conf);
	      }
	      return static_sc;
	   }
	   */
	
	static SparkConf conf = new SparkConf().setAppName("Testing").setMaster("local[2]");
	public static JavaSparkContext sc = new JavaSparkContext(conf);
}
