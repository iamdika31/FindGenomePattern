package com.genomePattern;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;



import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


class GetPattern implements  Function<String, List<String>>{
	Pattern p = Pattern.compile("GCAGTTAGG|TTAGGCAGT|GCAGT|TTAGGCA|AGTTAGG|GCA|AGT|TTAGG");
	public List<String> call(String s){
		Matcher m = p.matcher(s);
		List<String> x = new ArrayList<>();
		while(m.find()) {
			if(m.group().contains("GCA")) {
				x.add("GCA");
			}
			if(m.group().contains("AGT")) {
				x.add("AGT");
			}
			if(m.group().contains("TTAGG")) {
				x.add("TTAGG");
			}
		}
		return x;
	}
}

public class Genome 
{
    public static void main( String[] args )
    {
       	Logger.getLogger("org.apache").setLevel(Level.WARN);;
    	SparkConf conf = new SparkConf().setAppName("genom pattern").setMaster("local[*]");
//    	
    	JavaSparkContext sc = new JavaSparkContext(conf);
    	JavaRDD<String> distText = sc.textFile("src/main/resources/data_genome.txt"); 	
    	JavaRDD<List<String>> patternRDD = distText.map(new GetPattern());
    	JavaRDD<String> flattenRDD = patternRDD.flatMap(
    			new FlatMapFunction<List<String>,String>() {
    				 public Iterator<String> call(List<String> li) throws Exception {
    				        return li.iterator();
    				 }
    			}
    		);    	
		JavaPairRDD<String, Integer> res= flattenRDD.mapToPair(value1 -> new Tuple2<String, Integer>(value1,1))
													.reduceByKey((value1,value2) -> value1 + value2);
    	System.out.println(res.collect());
    	sc.close();
    }
}
