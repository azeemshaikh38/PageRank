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

public class PageRank {

  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      String[] line = value.toString().split(" --->");
      if (line.length >= 2) { //Avoid lines not in xml format
      	String keyString = line[0];
      	String valueString = line[1];

	//Split using XML tags <pr> and <urlLinks> 
	String[] prString = valueString.split("</?pr>");
      	String[] urlList = valueString.split("</?urlLinks>");
	
	if ((prString.length >= 2) && (urlList.length >= 2)) { //Skip unknown lines
		float pr = Float.parseFloat(prString[1]);
		//Generate (URL, urlList)
      		output.collect(new Text(keyString), new Text("<urlLinks>"+urlList[1]+"</urlLinks>"));

      		Matcher matchedURLs = Pattern.compile("\\[\\[[^\\[\\[]+\\]\\]").matcher(urlList[1]);
      		int matchCount = 0;
      		List<String> matchedStrings = new LinkedList<String>();
      		while (matchedURLs.find()) {
			matchedStrings.add(matchedURLs.group().replaceAll("\\[\\[|\\]\\]", "").replaceAll("\\s+",""));
			matchCount++;
      		}
		if (matchCount != 0) { //Dangling Nodes
		      	float prNew = (float)(pr*10000/(float)matchedStrings.size());
		      	Iterator listItr = matchedStrings.iterator();
		      	while (listItr.hasNext()) //Generate (url1, PR1), (url2, PR2)....
				output.collect(new Text(listItr.next().toString()), new Text(Float.toString(prNew)));
		}
	}
      }
    }
  }

  public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      float sum = 0.0f; 
      String pr = ""; String[] urlList = new String[5];
      urlList[1] = "";
      Hashtable<String, Integer> titleExists = new Hashtable<String, Integer>();
      List<String> incomingURLs = new LinkedList<String>();
      while (values.hasNext()) {
	String temp = values.next().toString();
	if (temp.matches("^<urlLinks>.*")) {
		//List of URLs
		urlList = temp.split("</?urlLinks>");
	} else {
		//Summation
        	sum += (float) Float.parseFloat(temp);
        	pr += temp+", ";
	}
      }
      sum = sum/10000f;
      //Calculate new PR
      float prNew = ((0.15f)+(0.85f*sum));
      output.collect(new Text(key.toString()+" --->"), new Text("<pr>"+Float.toString(prNew)+"</pr><urlLinks>"+urlList[1]+"</urlLinks>"));
    }
  }

  public static void main(String[] args) throws Exception {
    /*Phase 1 of PageRank Algorithm*/
    JobConf conf = new JobConf(urlLinks.class);
    conf.setJobName("Phase1");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapperClass(urlLinks.Map.class);
    //conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(urlLinks.Reduce.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    JobClient.runJob(conf);

    String inputDir = args[1];
    String outputDir = "/user/mshaikh4/PageRank/outputPhase2";
    String outputDirOrig = "/user/mshaikh4/PageRank/outputPhase2";

    /*Start Phase 2 of PageRank*/    
    for (int i =0; i <10; i++) {
    	JobConf conf2 = new JobConf(PageRank.class);
    	conf2.setJobName("Phase2");

    	conf2.setOutputKeyClass(Text.class);
    	conf2.setOutputValueClass(Text.class);

    	conf2.setMapperClass(Map.class);
    	conf2.setReducerClass(Reduce.class);
    
    	conf2.setInputFormat(TextInputFormat.class);
    	conf2.setOutputFormat(TextOutputFormat.class);
   
    	FileInputFormat.setInputPaths(conf2, new Path(inputDir));
    	FileOutputFormat.setOutputPath(conf2, new Path(outputDir));
    
    	JobClient.runJob(conf2);
 
        inputDir = outputDir;
	outputDir = outputDirOrig+"_"+Integer.toString(i);
    }
  }
}
