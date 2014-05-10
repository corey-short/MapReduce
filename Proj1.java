import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.lang.Math;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/*
 * This is the skeleton for CS61c project 1, Fall 2013.
 *
 * Reminder:  DO NOT SHARE CODE OR ALLOW ANOTHER STUDENT TO READ YOURS.
 * EVEN FOR DEBUGGING. THIS MEANS YOU.
 *
 */
public class Proj1{

    /*
     * Inputs is a set of (docID, document contents) pairs.
     */
    static int count_index_output = 0;

    public static class Map1 extends Mapper<WritableComparable, Text, Text, DoublePair>{
        /** Regex pattern to find words (alphanumeric + _). */
        final static Pattern WORD_PATTERN = Pattern.compile("\\w+");

        private String targetGram = null;
        private int funcNum = 0;

        /*
         * Setup gets called exactly once for each mapper, before map() gets called the first time.
         * It's a good place to do configuration or setup that can be shared across many calls to map
         */
        @Override
        public void setup(Context context) {
            targetGram = context.getConfiguration().get("targetWord").toLowerCase();
            try {
                funcNum = Integer.parseInt(context.getConfiguration().get("funcNum"));
            }
			catch (NumberFormatException e) {
                /* Do nothing. */
            }
        }

        @Override
        public void map(WritableComparable docID, Text docContents, Context context)
			throws IOException, InterruptedException {
            Matcher matcher = WORD_PATTERN.matcher(docContents.toString());
            Func func = funcFromNum(funcNum);

            // YOUR CODE HERE
	
			int index = 0;
			ArrayList<Integer> targetIndex = new ArrayList();
			while (matcher.find()) {
				String word = matcher.group().toLowerCase();
				if (word.equals(targetGram.toLowerCase())) {
					targetIndex.add(index);
				}
				index++;
				
			}
			double d; 
			int n = 0;
			Matcher match = WORD_PATTERN.matcher(docContents.toString());
			while (match.find()) {
				if (targetIndex.size() == 0) {
					d = Double.POSITIVE_INFINITY;
				}
				else {
					d = Math.abs(targetIndex.get(0) - n);
					
					for(int i = 1; i < targetIndex.size(); i++) {
					d = Math.min(d, Math.abs(targetIndex.get(i) - n));				
					}
				}
				if (d != 0) {
					String word2 = match.group().toLowerCase();
					Text text = new Text(word2);
					context.write(text, new DoublePair(1, func.f(d)));
				}
				n++;
			}
	}
			

			/**
			* A LinkedHashMap to store words in the document as Strings and to store
			* their respective indices as Doubles in an ArrayList. This is used to 
			* calculate the min_distance, d, as part of the DoublePair output for the
			* mapper. This implementation is basically a linked version of Google's 
			* multimap. Test code is in Test.java. Which is not provided because I switched to
			* a single arraylist implementation,
			* @author Short
			*/

			/*Map<String, ArrayList<Double>> hash
							= new LinkedHashMap<String, ArrayList<Double>>();
			double index = 0.0;
			double d;
			while (matcher.find()) {
				String word = matcher.group().toLowerCase();
				if (!hash.containsKey(word))
					hash.put(word, new ArrayList<Double>());
				hash.get(word).add(index);
				index++;
			}
			Matcher match = WORD_PATTERN.matcher(docContents.toString());
				if (!hash.containsKey(targetGram)) {
					d = Double.POSITIVE_INFINITY;
				}
				else {
			 		for (String key: hash.keySet()) {
						if (key != targetGram) {
							int word_index = 0;
							int target_index = 0;
							d = Math.abs(hash.get(key).get(word_index)
							- hash.get(targetGram).get(target_index));
							target_index++;
							while (word_index < hash.get(key).size()) {
								while (target_index < hash.get(targetGram).size()) {
									double temp = 
									Math.abs(hash.get(key).get(word_index) - 
									hash.get(targetGram).get(target_index));
									d = Math.min(d, temp);
									target_index++;
								}
								context.write(new Text(key), new DoublePair(1.0, func.f(d)));
								word_index++;
							}
						}
					}	
			 	}
     			
	}*/
			

        /** Returns the Func corresponding to FUNCNUM*/
        private Func funcFromNum(int funcNum) {
            Func func = null;
            switch (funcNum) {
                case 0:	
                    func = new Func() {
                        public double f(double d) {
                            return d == Double.POSITIVE_INFINITY ? 0.0 : 1.0;
                        }			
                    };	
                    break;
                case 1:
                    func = new Func() {
                        public double f(double d) {
                            return d == Double.POSITIVE_INFINITY ? 0.0 : 1.0 + 1.0 / d;
                        }			
                    };
                    break;
                case 2:
                    func = new Func() {
                        public double f(double d) {
                            return d == Double.POSITIVE_INFINITY ? 0.0 : 1.0 + Math.sqrt(d);
                        }			
                    };
                    break;
            }
            return func;
        }
    }

    /** Here's where you'll be implementing your combiner.
	It must be non-trivial for you to receive credit. */
    public static class Combine1 extends Reducer<Text, DoublePair, Text, DoublePair> {

        @Override
        public void reduce(Text key, Iterable<DoublePair> values,
            Context context) throws IOException, InterruptedException {
				
            // YOUR CODE HERE
			// This is the combiner
			double sum_num_count = 0;
			double sum_d_count = 0;	

			for (DoublePair value : values) {
				sum_num_count += value.getDouble1();
				sum_d_count += value.getDouble2();
			}
			context.write(key, new DoublePair(sum_num_count, sum_d_count));
		}
    }


    public static class Reduce1 extends Reducer<Text, DoublePair, DoubleWritable, Text> {
        @Override
        public void reduce(Text key, Iterable<DoublePair> values,
            Context context) throws IOException, InterruptedException {

			// YOUR CODE HERE
		
			double a_word = 0;
			double s_word = 0;
			double corate;

			for (DoublePair value : values) {
				// sum occurances of word for A_w double1
				a_word += value.getDouble1(); 
				// sum of func.f(d_word) over all occurance of word
				s_word += value.getDouble2();
			}
			// calculate co-occurance rate by given forumla
			if (s_word > 0) {
				corate = -1 * (s_word * Math.pow(Math.log(s_word), 3)) / a_word;
			}
			else {
				corate = 0;
			}
			context.write(new DoubleWritable(corate), key); 
		}
    }

    public static class Map2 extends Mapper<DoubleWritable, Text, DoubleWritable, Text> {
        //maybe do something, maybe don't
    }

    public static class Reduce2 extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {

        int n = 0;
        static int N_TO_OUTPUT = 100;

        /*
         * Setup gets called exactly once for each reducer, before reduce() gets called the first time.
         * It's a good place to do configuration or setup that can be shared across many calls to reduce
         */
        @Override
        protected void setup(Context c) {
            n = 0;
        }

        /*
         * Your output should be a in the form of (DoubleWritable score, Text word)
         * where score is the co-occurrence value for the word. Your output should be
         * sorted from largest co-occurrence to smallest co-occurrence.
         */
        @Override
        public void reduce(DoubleWritable key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException {

            // YOUR CODE HERE
			int i = 0;
			double s;
			s = Math.abs(key.get());
			for (Text value : values) {
				if (count_index_output < N_TO_OUTPUT) {
					context.write(new DoubleWritable(s), value); 
            			}
				count_index_output++;
			}
	}
  }

    /*
     *  You shouldn't need to modify this function much. If you think you have a good reason to,
     *  you might want to discuss with staff.
     *
     *  The skeleton supports several options.
     *  if you set runJob2 to false, only the first job will run and output will be
     *  in TextFile format, instead of SequenceFile. This is intended as a debugging aid.
     *
     *  If you set combiner to false, the combiner will not run. This is also
     *  intended as a debugging aid. Turning on and off the combiner shouldn't alter
     *  your results. Since the framework doesn't make promises about when it'll
     *  invoke combiners, it's an error to assume anything about how many times
     *  values will be combined.
     */
    public static void main(String[] rawArgs) throws Exception {
        GenericOptionsParser parser = new GenericOptionsParser(rawArgs);
        Configuration conf = parser.getConfiguration();
        String[] args = parser.getRemainingArgs();

        boolean runJob2 = conf.getBoolean("runJob2", true);
        boolean combiner = conf.getBoolean("combiner", true);

        System.out.println("Target word: " + conf.get("targetWord"));
        System.out.println("Function num: " + conf.get("funcNum"));

        if(runJob2)
            System.out.println("running both jobs");
        else
            System.out.println("for debugging, only running job 1");

        if(combiner)
            System.out.println("using combiner");
        else
            System.out.println("NOT using combiner");

        Path inputPath = new Path(args[0]);
        Path middleOut = new Path(args[1]);
        Path finalOut = new Path(args[2]);
        FileSystem hdfs = middleOut.getFileSystem(conf);
        int reduceCount = conf.getInt("reduces", 32);

        if(hdfs.exists(middleOut)) {
            System.err.println("can't run: " + middleOut.toUri().toString() + " already exists");
            System.exit(1);
        }
        if(finalOut.getFileSystem(conf).exists(finalOut) ) {
            System.err.println("can't run: " + finalOut.toUri().toString() + " already exists");
            System.exit(1);
        }

        {
            Job firstJob = new Job(conf, "job1");

            firstJob.setJarByClass(Map1.class);

            /* You may need to change things here */
            firstJob.setMapOutputKeyClass(Text.class);
            firstJob.setMapOutputValueClass(DoublePair.class);
            firstJob.setOutputKeyClass(DoubleWritable.class);
            firstJob.setOutputValueClass(Text.class);
            /* End region where we expect you to perhaps need to change things. */

            firstJob.setMapperClass(Map1.class);
            firstJob.setReducerClass(Reduce1.class);
            firstJob.setNumReduceTasks(reduceCount);


            if(combiner)
                firstJob.setCombinerClass(Combine1.class);

            firstJob.setInputFormatClass(SequenceFileInputFormat.class);
            if(runJob2)
                firstJob.setOutputFormatClass(SequenceFileOutputFormat.class);

            FileInputFormat.addInputPath(firstJob, inputPath);
            FileOutputFormat.setOutputPath(firstJob, middleOut);
            firstJob.waitForCompletion(true);
        }

        if(runJob2) {
            Job secondJob = new Job(conf, "job2");

            secondJob.setJarByClass(Map1.class);
            /* You may need to change things here */
            secondJob.setMapOutputKeyClass(DoubleWritable.class);
            secondJob.setMapOutputValueClass(Text.class);
            secondJob.setOutputKeyClass(DoubleWritable.class);
            secondJob.setOutputValueClass(Text.class);
            /* End region where we expect you to perhaps need to change things. */

            secondJob.setMapperClass(Map2.class);
            secondJob.setReducerClass(Reduce2.class);

            secondJob.setInputFormatClass(SequenceFileInputFormat.class);
            secondJob.setOutputFormatClass(TextOutputFormat.class);
            secondJob.setNumReduceTasks(1);


            FileInputFormat.addInputPath(secondJob, middleOut);
            FileOutputFormat.setOutputPath(secondJob, finalOut);

            secondJob.waitForCompletion(true);
        }
    }

}
