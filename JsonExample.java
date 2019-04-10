package edu.nyu.tandon.bigdata.hadoop;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.*;

public class JsonExample extends Configured implements Tool{

    /**
     * Main function which calls the run method and passes the args using ToolRunner
     * @param args Two arguments input and output file paths
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new JsonExample(), args);
        System.exit(exitCode);
    }

    /**
     * Run method which schedules the Hadoop Job
     * @param args Arguments passed in main function
     */
    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.printf("Usage: %s needs two arguments <input> <output>\n",
                    getClass().getSimpleName());
            return -1;
        }

        //Initialize the Hadoop job and set the jar as well as the name of the Job
        Job job = new Job();
        job.setJarByClass(JsonExample.class);
        job.setJobName("Juan's JsonExample - HW1 - Q5");

        // output spec; count number of top level json records
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // input file
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(NYUJsonInputFormat.class);

        //Set the MapClass
        job.setMapperClass(MyMapper.class);

        // this example uses NO reducer
//        job.setReducerClass(MyReducer.class);

        // want answer in a single file
        job.setNumReduceTasks(1);

        //Wait for the job to complete and print if the job was successful or not
        int returnValue = job.waitForCompletion(true) ? 0:1;

        if(job.isSuccessful()) {
            System.out.println("Job was successful");
        } else if(!job.isSuccessful()) {
            System.out.println("Job was not successful");
        }
        return returnValue;
    }

    // json record count mapper
    public static class MyMapper extends Mapper<Object, Text, Text, Text> {

        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            // the value has to be a well formed single json record, verify correctness

            // NOTE **** this third party library has to be available in all nodes
            try {
                JSONObject obj = new JSONObject(value.toString());

                // let's extract a field: URL, for this example
                context.write(new Text("url"), new Text((String)obj.get("url")));
            }
            catch (JSONException e) {
                // bad record parse; will ignore
            }
        }
    }


}
