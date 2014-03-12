import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Make inference as to what sport someone would be watching during the London 2012 
 * olympics based on the hash tagged athletes in their tweets.
 * Made use of Hadoop's distributed cache to distribute athlete data among the data nodes
 * executing this job in order to perform an in-map join 
 * @author kevingracie
 *
 */


public class SportAthlete {


	public static class SportAthleteMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		
		private Text userId = new Text();
		private Text sportText = new Text();
		private Path[] distFiles;
		private Path athletesFilePath;
		private Map<String, String> athletesMap;
		
		
		protected void setup(Context context) throws IOException, InterruptedException {
			distFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			athletesFilePath = distFiles[0];
			athletesMap = new HashMap<String, String>();
			try {
				loadAthleteData();
			}
			catch(Exception e) {
				System.out.println("Exception "+e);
			}
		}
		
		private void loadAthleteData() throws Exception {
			BufferedReader reader = new BufferedReader(new FileReader(athletesFilePath.toString()));
			try {
				String line = "";
				while(reader.readLine() != null) {
					line = reader.readLine();
					String[] lineSplit = line.toString().split(",");
					String[] nameSplit = lineSplit[0].split(" ");
					String joinedName = nameSplit[1]+nameSplit[0];
					athletesMap.put(joinedName.toLowerCase(), lineSplit[2]);
				}
			}
			finally {
				reader.close();
			}
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] splits = value.toString().split("¿");
			
			String hashtag = "";
			Set<String> athleteSet = new HashSet<String>();
			athleteSet = athletesMap.keySet();
			Iterator<String> itr = athleteSet.iterator();
			
			if(splits.length > 2) {
				userId.set(splits[1]);
				hashtag = splits[2];
				StringTokenizer tokenizer = new StringTokenizer(splits[2]);
				while(tokenizer.hasMoreTokens()) {
					String temp = tokenizer.nextToken();
					while(itr.hasNext()) {
						String athleteName = (String)itr.next();
						if(temp.toLowerCase().contains(athleteName.toLowerCase())) {
							sportText.set(athletesMap.get(athleteName));
							context.write(userId, sportText);
						}
					}
				}
			}
		}
	}
	
	public static class SportAthleteReducer extends Reducer<Text, Text, Text, Text> {
		
		private Text word = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String temp = "";
			for(Text val : values) {
				temp += val.toString()+" ";
			}
			System.out.println("key = "+key+" and value = "+temp);
			word.set(temp);
			context.write(key, word);
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		DistributedCache.addCacheFile(new Path("AthleteInput/Athletes.csv").toUri(), conf);
		Job job = new Job(conf, "sportathlete");
		job.setJarByClass(SportAthlete.class);
		job.setMapperClass(SportAthleteMapper.class);
		job.setReducerClass(SportAthleteReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
}
