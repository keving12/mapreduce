package com.kaygee.support.mapreduce;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;
import org.apache.cassandra.hadoop.ColumnFamilyOutputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;

/**
 * Simple MapReduce job that goes through records stored in an Apache 
 * Cassandra Key-Value store and identifies all the records with a value of 
 * resolved for a particular clientId. 
 * 
 * @author kevingracie
 *
 */

public class MostResolvingUser extends Configured implements Tool {
	
	protected static Log logger = LogFactory.getLog(MostResolvingUser.class);

	//Hadoop mapper type - <input key type, input value type, output key type, output value type>
	public static class Map extends Mapper<ByteBuffer, SortedMap<ByteBuffer, IColumn>, Text, IntWritable> {
		
		private Text resolvedByText = new Text();
		private IntWritable one = new IntWritable(1);
		private ByteBuffer clientIdBuffer, statusBuffer, resolvedByBuffer;
		private IColumn clientIdColumn, statusColumn, resolvedByColumn;
		private String clientId;
		
		/* Cassandra stores values in byte arrays so we use ByteBufferUtil to read bytes arrays for values
		 * we are interested in into ByteBuffer 
		 */
		protected void setup(Context context) {
			clientIdBuffer = ByteBufferUtil.bytes("clientId");
			statusBuffer = ByteBufferUtil.bytes("status");
			resolvedByBuffer = ByteBufferUtil.bytes("resolvedBy");
		}
		
		
		public void map(ByteBuffer key, SortedMap<ByteBuffer, IColumn> columns, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			clientId = conf.get("clientId");
			clientIdColumn = columns.get(clientIdBuffer);
			statusColumn = columns.get(statusBuffer);
			resolvedByColumn = columns.get(resolvedByBuffer);

			
			String clientIdValue = ByteBufferUtil.string(clientIdColumn.value());
			String statusValue = ByteBufferUtil.string(statusColumn.value());
			
			
			if(clientIdValue.equalsIgnoreCase(clientId) && statusValue.equalsIgnoreCase("resolved")) {
				String resolvedByValue = ByteBufferUtil.string(resolvedByColumn.value());
				resolvedByText.set(resolvedByValue);
				context.write(resolvedByText, one);
			}
		}
	}
	
	public static class CassandraReducer extends Reducer<Text, IntWritable, ByteBuffer, List<Mutation>> {
		private ByteBuffer outputKey;
		
		protected void setup(Context context) {
			Configuration conf = context.getConfiguration();
			outputKey = ByteBufferUtil.bytes(conf.get("reportName"));
		}
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable val : values) {
				sum += val.get();
			}
			context.write(outputKey, Collections.singletonList(getMutation(key, sum)));
		}
		
		private static Mutation getMutation(Text word, int sum) {
			Column c = new Column();
			c.setName(Arrays.copyOf(word.getBytes(), word.getLength()));
			c.setValue(ByteBufferUtil.bytes(String.valueOf(sum)));
			c.setTimestamp(System.currentTimeMillis());
			
			Mutation m = new Mutation();
			m.setColumn_or_supercolumn(new ColumnOrSuperColumn());
			m.column_or_supercolumn.setColumn(c);
			return m;
		}
	}
	
	public int run(String[] args) throws Exception {
		
		final String[] myArgs = args.clone();
		
		UserGroupInformation ugi = UserGroupInformation.createProxyUser("projuser", UserGroupInformation.getLoginUser());
		ugi.doAs(new PrivilegedExceptionAction<Void>() {
	          public Void run() throws Exception {
	        	  Configuration conf = new Configuration();
	        	  
	        	  conf.set("fs.default.name", "hdfs://192.168.0.11:54310");
	        	  conf.set("mapred.job.tracker", "192.168.0.11:54311");
	        	  conf.set("clientId", myArgs[0]);
	        	  conf.set("reportName", myArgs[1]);
	        	  conf.set("dateFrom", myArgs[2]);
	        	  conf.set("dateTo", myArgs[3]);
	        	  
	        	  Job job = new Job(conf, "mostresolving");
	        	  
	        	  job.setJarByClass(MostResolvingUser.class);
	        	  job.setMapperClass(Map.class);
	        	  job.setReducerClass(CassandraReducer.class);
	        	  job.setMapOutputKeyClass(Text.class);
	        	  job.setMapOutputValueClass(IntWritable.class);
	        	  job.setOutputKeyClass(ByteBuffer.class);
	        	  job.setOutputValueClass(List.class);
	        	  job.setInputFormatClass(ColumnFamilyInputFormat.class);
	        	  job.setOutputFormatClass(ColumnFamilyOutputFormat.class);
					
	        	  ConfigHelper.setRpcPort(job.getConfiguration(), "9160");
	        	  ConfigHelper.setInitialAddress(job.getConfiguration(), "localhost");
	        	  ConfigHelper.setPartitioner(job.getConfiguration(), "org.apache.cassandra.dht.RandomPartitioner");
	        	  ConfigHelper.setInputColumnFamily(job.getConfiguration(), "bugmanager", "Tickets");
	        	  ConfigHelper.setOutputColumnFamily(job.getConfiguration(), "bugmanager", "ResolvingUserData");
	        	  SlicePredicate predicate = new SlicePredicate().setColumn_names(Arrays.asList(
	        			  ByteBufferUtil.bytes("clientId"), 
	        			  ByteBufferUtil.bytes("status"), 
	        			  ByteBufferUtil.bytes("resolvedBy")
	        			  ));
	        	  ConfigHelper.setInputSlicePredicate(job.getConfiguration(), predicate);
					
	        	  job.waitForCompletion(false);
	        	  return null;
	          }
		});
		return 0;
	}
	
}
