package edu.nyu.tandon.bigdata.hadoop;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.*;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR;

/*** Custom Hadoop Record Reader : zipped file
 *
 * We want to produce (K,V) pairs where
 * K = filename inside the zip file
 * V = bytes corresponding to the file
 *
 * ***/
 public class NYUJsonRecordReader extends RecordReader<Text, Text> {

     protected Text key;
     protected Text value;
     protected int numEntries;
     protected Reader inputStreamReader;
     protected InputStream fin;
     protected FileSplit split;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {

        split = (FileSplit) inputSplit;

        // open the input as a stream
        // ---------------------------

        // we can get the filename from the job invocation in this case
        String fileName = context.getConfiguration().get(INPUT_DIR, "").replace("file:/","");
        fin = new FileInputStream(fileName);
        // or derive it from the split; for this example, they are identical
//        fin = new FileInputStream(split.getPath().getName().replaceAll("file:/","");

        inputStreamReader = new InputStreamReader(fin);

        // we know how many records we have: 1
        // in real world, this will nto be true
        numEntries = 1;

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        if (numEntries == 0) return false;

        // buffer one record
        char[] buffer = new char[(int)split.getLength()];

        // read the record
        try {
            if (split.getStart() != 0)
                inputStreamReader.skip((int)split.getStart());
            int len = inputStreamReader.read(buffer,0,(int)split.getLength());
            if (len == -1) return false;
        }
        catch (Exception e) {
            return false;
        }

        // save the current key: anything works for our exmaple
        key = new Text("JSON Record");

        // save the current value
        value = new Text(new String(buffer));

        numEntries--;
        return true;

    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        // we only have one record; so we are done
        return 1;
    }

    @Override
    public void close() throws IOException {
        inputStreamReader.close();
        fin.close();
    }

}
