package com.rishabh.bigdata.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import com.rishabh.bigdata.log.Logger;

public class HadoopReducer extends MapReduceBase implements
		Reducer<IntWritable, Text, Text, IntWritable> {

	public void reduce(IntWritable key, Iterator<Text> values,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		int total_count = 0;
		
		System.out.println("into reducer");

		Text mOutputText = new Text();
		
		while(values.hasNext()) {
			total_count++;
			mOutputText.set(values.next());
		}
		
		output.collect(mOutputText, new IntWritable(total_count));
		
		Logger.getInstance().logInfo("Hadoop Reducer", output.toString());
	}
}
