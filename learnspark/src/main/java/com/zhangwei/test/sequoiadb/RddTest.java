package com.zhangwei.test.sequoiadb;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.BSONObject;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.sequoiadb.hadoop.io.BSONWritable;
import com.sequoiadb.hadoop.mapreduce.SequoiadbInputFormat;

public class RddTest {
	public static void main(String[] args) throws Exception {
	    String master;
	    if (args.length > 0) {
		    master = args[0];
	    } else {
		    master = "local";
	    }
	    
	    JavaSparkContext sc = new JavaSparkContext(
	    	      master, "basicavg", System.getenv("SPARK_HOME"), System.getenv("JARS"));
	    
	    Configuration config = new Configuration();
		
	    JavaPairRDD rdd1 = sc.newAPIHadoopRDD(config , SequoiadbInputFormat.class, Object.class, BSONWritable.class);
		
		
		
		/**
		 * 这里用了新的API，并且MongoInputFormat必须从com.mongodb.hadoop导入。对于旧的API，你应该使用hadoopRDD方法和com.mongodb.hadoop.mapred.MongoInputFormat。
		 * */
		JavaPairRDD rdd2 = sc.newAPIHadoopRDD(config , MongoInputFormat.class, Object.class, BSONObject.class);
		
		rdd2.saveAsNewAPIHadoopFile("file:///bogus", Object.class, Object.class, MongoOutputFormat.class, config);
	}
}
