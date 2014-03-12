import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * MapReduce job to provide some inference of the likely sport someone
 * was watching during the London 2012 olympics based on hash tags present in their tweets.
 * Made use of Hadoop's distributed cache to distribute a file on which to join tweet data to 
 * each of the data nodes executing this job.
 *  
 * @author kevingracie
 *
 */

public class SportHashTag {
	
	public static class SportHashTagMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		private Text userId = new Text();
		private Text sportText = new Text();
		private Path[] distFiles;
		private Path sportsFilePath;
		private Set<String> sportSet;
		
		protected void setup(Context context) throws IOException, InterruptedException {
			distFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			sportsFilePath = distFiles[0];
			sportSet = new HashSet<String>();
			try {
				loadSportData();
			}
			catch(Exception e) {
				System.out.println("Exception "+e);
			}
		}
		
		private void loadSportData() throws Exception {
			BufferedReader reader = new BufferedReader(new FileReader(sportsFilePath.toString()));
			try {
				String line = "";
				while(reader.readLine() != null) {
					line = reader.readLine();
					String[] lineSplit = line.toString().split(",");
					String sport = lineSplit[0];
					sportSet.add(sport);
				}
			}
			finally {
				reader.close();
			}
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] splits = value.toString().split("À");
			
			String hashtag = "";
			Iterator<String> itr = sportSet.iterator();
			
			if(splits.length > 2) {
				userId.set(splits[1]);
				hashtag = splits[2];
				StringTokenizer tokenizer = new StringTokenizer(splits[2]);
				while(tokenizer.hasMoreTokens()) {
					String temp = tokenizer.nextToken();
					while(itr.hasNext()) {
						String sportName = (String)itr.next();
						if(temp.toLowerCase().contains(sportName.toLowerCase())) {
							sportText.set(sportName);
							context.write(userId, sportText);
						}
					}
				}
			}
		}
		
	}
	
	public static class SportHashTagReducer extends Reducer<Text, Text, Text, Text> {
		
		private Text word = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String temp = "";
			for(Text val : values) {
				temp += val.toString()+" ";
			}
			word.set(temp);
			context.write(key, word);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		DistributedCache.addCacheFile(new Path("SportInput/Sports.csv").toUri(), conf);
		Job job = new Job(conf, "sporthashtag");
		job.setJarByClass(SportHashTag.class);
		job.setMapperClass(SportHashTagMapper.class);
		job.setReducerClass(SportHashTagReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}

}
