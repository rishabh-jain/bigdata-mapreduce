package com.rishabh.bigdata.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import com.mongodb.hadoop.MongoConfig;
import com.mongodb.hadoop.mapred.MongoOutputFormat;
import com.mongodb.hadoop.util.MongoTool;
import com.rishabh.bigdata.log.Logger;

public class MapredDriver {

	public static void main(String[] args) {
		try {
			Configuration mConfig = new Configuration();

			MongoTool mMongoTool = new MongoTool();
			MongoConfig mMongoConfig = new MongoConfig(mConfig);

			mMongoTool.setConf(mConfig);
			mMongoConfig
					.setOutputURI("mongodb://sanskritii.com:27017/BigData.test.out"); // writes
																						// data
																						// back
																						// to
																						// mongo
																						// collection

			JobConf mJob = new JobConf(MapredDriver.class);

			mJob.addResource(mConfig);

			mJob.setJobName("Hadoop File");

			FileInputFormat.setInputPaths(mJob, new Path(args[0]));

			mJob.setMapperClass(HadoopMapper.class);
			mJob.setReducerClass(HadoopReducer.class);

			mJob.setMapOutputKeyClass(IntWritable.class);
			mJob.setMapOutputValueClass(Text.class);

			mJob.setOutputKeyClass(Text.class);
			mJob.setOutputValueClass(IntWritable.class);

			mJob.setOutputFormat(MongoOutputFormat.class);

			System.out.println("Sending Job for map-reduction");

			Logger.getInstance().logInfo("Hadoop Manager - Map Reduce",
					"Sending Job for map-reduction");

			JobClient.runJob(mJob);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
			Logger.getInstance().logError("Hadoop Manager - Map Reduce",
					e.getMessage());
		}
	}

}
