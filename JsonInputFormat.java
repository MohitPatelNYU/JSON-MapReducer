package edu.nyu.tandon.bigdata.hadoop;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR;

/**
 * Extends the basic FileInputFormat to accept json file.
 *
 *
 **/
public class NYUJsonInputFormat extends InputFormat<Text, Text> {

    private static String fileName;
    private static Path infile;

    @Override
    public List<InputSplit> getSplits(JobContext context)
            throws IOException {

        // The simplest solution that preserves the ability to parallelize work,
        // is to assign one json record per split .
        // NOTE I am not checking for too many errors here...

        fileName = context.getConfiguration().get(INPUT_DIR, "").replace("file:/","");
        infile = new Path(fileName);

        // create a collection of input splits
        List<InputSplit> splits = new ArrayList<InputSplit>();

        // buffer to hold one record
        StringBuilder buffer = new StringBuilder();

        // open the input as a stream
        // ---------------------------
        InputStream fin = new FileInputStream(fileName);
        Reader inputStreamReader = new InputStreamReader(fin);

        // do we validate the JSON file here,
        //  we let the mapper validate the individual Json record

        // simple json parser as a push stack
        // ---------------------------
        Stack<Integer> stack = new Stack<Integer>();

        // read all characters one by one
        int nextC = 0;
        char theChar = 0;
        boolean building = false;
        int numRecords = 0;
        int pos = -1;
        int startPos = 0;

        while (nextC != -1) {

            nextC = inputStreamReader.read();
            pos++;

            // find whether to push or pop a record delimiter (braces)
            if (nextC != -1) {

                theChar = (char) nextC;

                // start delimiter?
                if (theChar == '{') {
                    if (!building) startPos = pos;
                    building = true;
                    stack.push(1);
                }

                if (building) buffer.append(theChar);

                // end delimiter
                if (theChar == '}' && stack.size()>0) {
                    stack.pop();

                    // is it a complete record?
                    if (stack.size() == 0) {
                        building = false;
                    }
                }

                // completed record?
                if (!building && buffer.length()>0) {
                    numRecords++;
                    // create a split
                    splits.add(new FileSplit(infile, startPos, buffer.length(),null));
                    // clear the buffer
                    buffer.setLength(0);
                }
            }
        }

        inputStreamReader.close();
        fin.close();
        return splits;
    }

    /*** return next record, i.e. (K,V)
     *
     * @param split
     * @param context
     * @return (Text,BytesWritable)
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
    {
        return new NYUJsonRecordReader();
    }
}
