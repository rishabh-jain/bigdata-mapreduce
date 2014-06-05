package com.rishabh.bigdata.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.rishabh.bigdata.log.Logger;

public class HadoopMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, IntWritable, Text> {

	public void map(LongWritable key, Text value,
			OutputCollector<IntWritable, Text> output, Reporter reporter)
			throws IOException {

		System.out.println("into mapper");
		
		String mDataLine = value.toString();

		StringTokenizer mTokenizer = new StringTokenizer(mDataLine, ",");

		int token_number = 0;

		while (mTokenizer.hasMoreTokens()) {
			token_number++;
			output.collect(new IntWritable(token_number),
					new Text(mTokenizer.nextToken()));
		}

		Logger.getInstance().logInfo("Hadoop Mapper", output.toString());
		// super.map(key, value, context);
	}

}
