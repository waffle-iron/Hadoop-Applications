package com.miquel.mapreduce;

import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool {

	public static final String SALIDA_PROCESO = "salida";
	
	private static final Log LOG = LogFactory.getLog(Driver.class);
	
	public static void main(String[] args) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new Driver(), args);
		System.exit(res);
		
	}

	public int run(String[] args) throws Exception {
		
		Job job = Job.getInstance(getConf());
		
		job.setJarByClass(Driver.class);
		
		LOG.info("Comienza el Job : " + new Date());
	
		final String numeroReducers = job.getConfiguration().get("num-reducers");
		
		//Establecemos la ruta de entrada y salida del Job.
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//Anyadimos el fichero para salida al job.
		MultipleOutputs.addNamedOutput(job, SALIDA_PROCESO, TextOutputFormat.class, Text.class,
				Text.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		if (numeroReducers != null && numeroReducers.length() > 0) {
			job.setNumReduceTasks(Integer.valueOf(numeroReducers));
		} else {
			job.setNumReduceTasks(9);
		}

		job.waitForCompletion(true);

		LOG.info("Fin del Job : " + new Date());

		
		return 0;
	}

}
