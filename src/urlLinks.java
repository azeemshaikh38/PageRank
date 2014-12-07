package org.myorg;

import java.io.IOException;
import java.util.*;
import java.util.regex.*;
import javax.xml.xpath.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class urlLinks {

  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      String line = value.toString().replaceAll("^\\s+", "");
      //The 2nd element of below split will be the Title
      String[] titleSplitOnce = line.split("</?title>");

      //For obtaining text, the split is in two parts
      String[] textSplitOnce = line.split("<text[^>]*>");
      String[] textSplitTwice = textSplitOnce[1].split("</text>");

      //From the obatined text, search for all [[nodeName]] pattern.
      //Below pattern ignores all nested brackets inside [[ ]].
      Matcher matchedURLs = Pattern.compile("\\[\\[[^\\[\\[]+\\]\\]").matcher(textSplitTwice[0]);
      int matchCount = 0;
      while (matchedURLs.find()) {
	String matchedString = matchedURLs.group();
	//Remove whitespaces and convert to lowercase to maximise string matching during PageRank 
	String[] urlList = matchedString.replaceAll("\\[\\[|\\]\\]", "").split("\\|");
	for (int i=0; i<urlList.length; i++) {
		output.collect(new Text(titleSplitOnce[1].replaceAll("\\s+","").toLowerCase()+" --->"), new Text("[["+urlList[i].replaceAll("\\s+","").toLowerCase()+"]]"));
	}
	matchCount++;
      }
      if (matchCount == 0) //Avoid dangling nodes 
	output.collect(new Text(titleSplitOnce[1].replaceAll("\\s+","").toLowerCase()+" --->"), new Text(""));	
    }
  }

  public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      //Reduce function simply combines all URLs and assigns initial PageRank
      String sum = "";
      while (values.hasNext()) {
	String temp = values.next().toString();
       	sum += temp+", ";
      }
      output.collect(key, new Text("<pr>0.15</pr><urlLinks>"+sum+"</urlLinks>"));
    }
  }

  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(urlLinks.class);
    conf.setJobName("wordcount");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapperClass(Map.class);
    //conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    JobClient.runJob(conf);
  }
}
