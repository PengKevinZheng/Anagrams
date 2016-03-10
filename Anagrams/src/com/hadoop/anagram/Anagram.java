package com.hadoop.anagram;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class Anagram extends Configured implements Tool {
	
	//sorted each word and output: <sortedword  word>
	public static class AnagramMapper extends Mapper<LongWritable, Text, Text, Text > {
		
			public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
				
				String word = value.toString();
				
				/*//sort the word
				char [] stringArr = word.toCharArray();
				Arrays.sort(stringArr);
				
				// this is the sortedword
				String sortedword = new String(stringArr);*/
				
				String sortedword = RemoveSort(word);
				
				context.write(new Text(sortedword), new Text(word));
				
				
			}
		
	}
	
	
	// get all the word with the same sortedword, and out put the (sortedword  word1,word2,word3...)
	public static class AnagramReducer extends Reducer< Text, Text, Text, Text > {
			
			public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
				
				// get the first element of the iterable
				String anagram = values.iterator().next().toString();
				
				//connect each word
				for(Text value : values){
					String word_same_key = value.toString(); 
					anagram = anagram + "," + word_same_key;
				}
				
				context.write(key, new Text(anagram));
				
			}
	}
	
	public static String RemoveSort(String str){
		
		char[] c = str.toCharArray();
		
		Set<Character> set = new HashSet();
		
		//remove duplicates
		for(char single : c) {
			set.add(single);
		}
		
		
		Object[] arr = set.toArray();
		Arrays.sort(arr);
		
		String sss;
		String anagram = "";
		for(int i=0; i<arr.length;i++) {
			Object obj  = arr[i];
            sss  = obj.toString();
          //  System.out.println(sss);
            anagram += sss;
		}
		
		System.out.println(anagram);
		
		return anagram;
		
	}
	
	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		 Configuration conf = new Configuration();//��ȡ�����ļ�

			Path out = new Path(arg0[1]);
	        FileSystem hdfs = out.getFileSystem(conf);
	        if (hdfs.isDirectory(out)) {//ɾ���Ѿ����ڵ����Ŀ¼
	            hdfs.delete(out, true);
	        }

	        Job job = new Job(conf, "Anagram" );//�½�һ������
	        job.setJarByClass(Anagram.class);// ����

	        FileInputFormat.addInputPath(job, new Path(arg0[0]));// �ļ�����·��
	        FileOutputFormat.setOutputPath(job, new Path(arg0[1]));// �ļ����·��

	        job.setMapperClass(AnagramMapper.class);// Mapper
	        job.setReducerClass(AnagramReducer.class);// Reducer

	        job.setOutputKeyClass(Text.class);//������key����
	        job.setOutputValueClass(Text.class);//��������value����


		return job.waitForCompletion(true) ? 0 : 1;//�ȴ�����˳���ҵ
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		 try {
				//��������·�������·��
				String[] args0 = {
						"hdfs://192.168.10.11:9000/dajiangtai/word.txt",
		                "hdfs://192.168.10.11:9000/dajiangtai/sortedword/"
				};
		        	int res = ToolRunner.run(new Configuration(), new Anagram(), args0);
		        	System.exit(res);
		        } catch (Exception e) {
		        	e.printStackTrace();
		        }
		    }

	}


