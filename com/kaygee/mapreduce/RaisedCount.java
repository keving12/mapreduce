package com.kaygee.support.mapreduce;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

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

import com.kaygee.support.utils.DateUtils;

public class RaisedCount extends Configured implements Tool {
	
	protected static Log logger = LogFactory.getLog(RaisedCount.class);

	public static class Map extends Mapper<ByteBuffer, SortedMap<ByteBuffer, IColumn>, Text, IntWritable> {
		private Text priority = new Text();
		private IntWritable one = new IntWritable(1);
		private ByteBuffer clientIdBuffer, priorityBuffer, raisedOnBuffer;
		private String clientId, dateFrom, dateTo;
		private IColumn priorityColumn, clientIdColumn, raisedOnColumn;
		private TreeMap<Date, Integer> dateMap;
		
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			clientIdBuffer = ByteBufferUtil.bytes("clientId");
			priorityBuffer = ByteBufferUtil.bytes("priority");
			raisedOnBuffer = ByteBufferUtil.bytes("raisedOn");
			dateMap = DateUtils.getWeeksBetween(Long.parseLong(conf.get("dateFrom")), Long.parseLong(conf.get("dateTo")));
		}
		
		public void map(ByteBuffer key, SortedMap<ByteBuffer, IColumn> columns, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			clientId = conf.get("clientId");
			clientIdColumn = columns.get(clientIdBuffer); //clientId column of current key
			priorityColumn = columns.get(priorityBuffer); // priority column of current key
			raisedOnColumn = columns.get(raisedOnBuffer); // raisedOn column of current key
			DateFormat ticketFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm"); // DateFormat for tickets in the system where time is specified
			Calendar c = Calendar.getInstance();
			long ticketMillis = 0;
			String clientIdValue = ByteBufferUtil.string(clientIdColumn.value()); //get clientId from current column
			String priorityValue = ByteBufferUtil.string(priorityColumn.value()); // get priority from current column
			String raisedOnValue = ByteBufferUtil.string(raisedOnColumn.value()); // get raisedOn from current column
			try {
				c.setTime(ticketFormat.parse(raisedOnValue));
			}
			catch(ParseException e) {
				logger.error("ParseException "+e);
				
			}
			if(clientIdValue.equalsIgnoreCase(clientId)) {
				if(c.getTime().after(dateMap.firstKey())  && c.getTime().before(dateMap.lastKey())) {
					priority.set(priorityValue.substring(0, 1)+dateMap.lowerEntry(c.getTime()).getValue());
					context.write(priority, one);
				}
			}
			
		}
	}
	
	public static class CassandraReducer extends Reducer<Text, IntWritable, ByteBuffer, List<Mutation>> {
		private ByteBuffer outputKey;
		
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			outputKey = ByteBufferUtil.bytes(conf.get("reportName"));
		}
		
		public void reduce(Text word, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable val : values) {
				sum += val.get();
			}
			context.write(outputKey, Collections.singletonList(getMutation(word, sum)));
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
	        	  
	        	  Job job = new Job(conf, "raisedcount");
	        	  
	        	  job.setJarByClass(RaisedCount.class);
	        	  job.setMapperClass(Map.class);
	        	  job.setReducerClass(CassandraReducer.class);
	        	  job.setMapOutputKeyClass(Text.class);
	        	  job.setMapOutputValueClass(IntWritable.class);
	        	  job.setOutputKeyClass(ByteBuffer.class);
	        	  job.setOutputValueClass(List.class);
	        	  job.setInputFormatClass(ColumnFamilyInputFormat.class);
	        	  job.setOutputFormatClass(ColumnFamilyOutputFormat.class);
					
	        	  /** DO NOT USE getConf() - USE job.getConfiguration()*/
	        	  ConfigHelper.setRpcPort(job.getConfiguration(), "9160");
	        	  ConfigHelper.setInitialAddress(job.getConfiguration(), "localhost");
	        	  ConfigHelper.setPartitioner(job.getConfiguration(), "org.apache.cassandra.dht.RandomPartitioner");
	        	  ConfigHelper.setInputColumnFamily(job.getConfiguration(), "bugmanager", "Tickets");
	        	  ConfigHelper.setOutputColumnFamily(job.getConfiguration(), "bugmanager", "RaisedData");
	        	  SlicePredicate predicate = new SlicePredicate().setColumn_names(Arrays.asList(
	        			  ByteBufferUtil.bytes("priority"), 
	        			  ByteBufferUtil.bytes("clientId"), 
	        			  ByteBufferUtil.bytes("raisedOn")
	        			  ));
	        	  ConfigHelper.setInputSlicePredicate(job.getConfiguration(), predicate);
					
	        	  job.waitForCompletion(false);
	        	  return null;
	          }
		});
		return 0;
	}
	
}
