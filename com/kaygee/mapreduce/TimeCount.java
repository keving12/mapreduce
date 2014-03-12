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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;

import com.kaygee.support.utils.DateUtils;

public class TimeCount extends Configured implements Tool {

	protected static Log logger = LogFactory.getLog(TimeCount.class);
	
	public static class Map extends Mapper<ByteBuffer, SortedMap<ByteBuffer, IColumn>, Text, IntWritable> {
		private Text priority = new Text();
		private ByteBuffer clientIdBuffer, priorityBuffer, raisedOnBuffer, resolvedOnBuffer, statusBuffer;
		private String clientId, dateFrom, dateTo;
		private IColumn clientIdColumn, priorityColumn, raisedOnColumn, resolvedOnColumn, statusColumn;
		private DateFormat userFormat, ticketFormat;
		private Calendar c;
		private TreeMap<Date, Integer> dateMap;
		
		
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			clientId = conf.get("clientId");
			dateFrom = conf.get("dateFrom");
			dateTo = conf.get("dateTo");
			clientIdBuffer = ByteBufferUtil.bytes("clientId");
			priorityBuffer = ByteBufferUtil.bytes("priority");
			raisedOnBuffer = ByteBufferUtil.bytes("raisedOn");
			resolvedOnBuffer = ByteBufferUtil.bytes("resolvedOn");
			statusBuffer = ByteBufferUtil.bytes("status");
			
			userFormat = new SimpleDateFormat("dd/MM/yyyy"); // DateFormat for date parameters specified by user (defaults to 00:00)
			ticketFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm"); // DateFormat for tickets in the system where time is specified
			
			c = Calendar.getInstance();	
			dateMap = DateUtils.getWeeksBetween(Long.parseLong(dateFrom), Long.parseLong(dateTo));
		}
		
		
		public void map(ByteBuffer key, SortedMap<ByteBuffer, IColumn> columns, Context context) throws IOException, InterruptedException {
			clientIdColumn = columns.get(clientIdBuffer);
			priorityColumn = columns.get(priorityBuffer);
			raisedOnColumn = columns.get(raisedOnBuffer);
			resolvedOnColumn = columns.get(resolvedOnBuffer);
			statusColumn = columns.get(statusBuffer);
			
			String clientIdValue = ByteBufferUtil.string(clientIdColumn.value());
			String priorityValue = ByteBufferUtil.string(priorityColumn.value());
			String raisedOnValue = ByteBufferUtil.string(raisedOnColumn.value());
			String resolvedOnValue = ByteBufferUtil.string(resolvedOnColumn.value());
			String statusValue = ByteBufferUtil.string(statusColumn.value());
			Date raisedOn = null;
			Date resolvedOn = null;
			
			if(statusValue.equalsIgnoreCase("resolved") && clientIdValue.equalsIgnoreCase(clientId)) {
				try {
					c.setTime(ticketFormat.parse(raisedOnValue));
					raisedOn = c.getTime();
					c.setTime(ticketFormat.parse(resolvedOnValue));
					resolvedOn = c.getTime();
				}
				catch(ParseException e) {
					logger.error("Cannot parse Raised or Resolved Date");
				}
				if(resolvedOn.after(dateMap.firstKey())  && resolvedOn.before(new Date(dateMap.lastKey().getTime()+604800000))) {
					int numDaysTicketActive = DateUtils.getNumDaysBetween(raisedOn, resolvedOn);
					priority.set(priorityValue.substring(0, 1)+dateMap.lowerEntry(c.getTime()).getValue());
					context.write(priority, new IntWritable(numDaysTicketActive));
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
	        	  
	        	  Job job = new Job(conf, "timecount");
	        	  
	        	  job.setJarByClass(TimeCount.class);
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
	        	  ConfigHelper.setOutputColumnFamily(job.getConfiguration(), "bugmanager", "TicketDuration");
	        	  SlicePredicate predicate = new SlicePredicate().setColumn_names(Arrays.asList(
	        			  ByteBufferUtil.bytes("priority"), 
	        			  ByteBufferUtil.bytes("clientId"), 
	        			  ByteBufferUtil.bytes("raisedOn"),
	        			  ByteBufferUtil.bytes("resolvedOn"),
	        			  ByteBufferUtil.bytes("status")
	        			  ));
	        	  ConfigHelper.setInputSlicePredicate(job.getConfiguration(), predicate);
					
	        	  job.waitForCompletion(false);
	        	  return null;
	          }
		});
		return 0;
	}
}
