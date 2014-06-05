package com.rishabh.bigdata.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import com.rishabh.bigdata.log.Logger;

public class MapredDriver_old {

	public static void main(String[] args) {
		try {
			Configuration mConfig = new Configuration();
			/*FileSystem mFileSystem = DistributedFileSystem.get(mConfig);
			
			
			Path mHDFSFilePath = new Path(mFileSystem.getUri().toString()
					+ "/user/rishabh/KeySet_UserPreference.bd");

			Path mHDFSOutputFilePath = new Path(mFileSystem.getUri().toString()
					+ "/user/rishabh/KeySet_UserPreference.out");
			*/
			
			JobConf mJob = new JobConf(MapredDriver_old.class);
			
			mJob.addResource(mConfig);
			
			mJob.setJobName("Hadoop File");

			FileInputFormat.setInputPaths(mJob, new Path(args[0]));
			FileOutputFormat.setOutputPath(mJob, new Path(args[1]));

			mJob.setMapperClass(HadoopMapper.class);
			mJob.setReducerClass(HadoopReducer.class);

			mJob.setMapOutputKeyClass(IntWritable.class);
			mJob.setMapOutputValueClass(Text.class);
			
			mJob.setOutputKeyClass(Text.class);
			mJob.setOutputValueClass(IntWritable.class);

			System.out.println("Sending Job for map-reduction");
			
			Logger.getInstance().logInfo("Hadoop Manager - Map Reduce", "Sending Job for map-reduction");


			JobClient.runJob(mJob);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
			Logger.getInstance().logError("Hadoop Manager - Map Reduce", e.getMessage());
		}
	}

}
