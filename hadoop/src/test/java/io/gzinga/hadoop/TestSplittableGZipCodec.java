/*
 * Copyright 2015 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.gzinga.hadoop;

import io.gzinga.GZipInputStreamRandomAccess;
import io.gzinga.GZipOutputStreamRandomAccess;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

public class TestSplittableGZipCodec {
	/**
	 * Writes "This is line\n" 10,000 times to target/test/testfile1.gz (which will be created)
	 *
	 * @param conf tells which filesystem (fs.defaultFS) to write to
	 * @throws IOException
	 */
	private void writeThisIsLineToTestfile1(Configuration conf) throws IOException{
		FileSystem fs = FileSystem.get(conf);
		fs.mkdirs(new Path("target/test"));
		GZipOutputStreamRandomAccess gzip = new GZipOutputStreamRandomAccess(
				fs.create(new Path("target/test/testfile1.gz")));
		String str = "This is line\n";
		for(int i = 1; i <= 10000; i++) {
			gzip.write(str.getBytes());
			if(i % 100 == 0) {
				gzip.addOffset(i/100l);
			}
		}
		Assert.assertEquals(gzip.getOffsetMap().size(), 100);
		gzip.close();
	}

	private static final String SPLITTABLE_GZIP = "io.gzinga.hadoop.SplittableGZipCodec";
	private static final String TESTFILE_1_STR = "target/test/testfile1.gz";
	private static final String TESTFILE_2_STR = "target/test/testfile2";
	private static final Path TESTFILE_1 = new Path(TESTFILE_1_STR);
	private static final Path TESTFILE_2 = new Path(TESTFILE_2_STR);

	/**
	 * Return a word-count job reading from target/test/testfile1.gz and writing to target/test/testfile2
	 * @param conf Configuation for the job
	 * @return The job doing the word count
	 * @throws IOException
	 */
	private Job wordCountTestfile1To2Job(Configuration conf) throws IOException{
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(WordCount.TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, TESTFILE_1);
		FileOutputFormat.setOutputPath(job, TESTFILE_2);
		return job;
	}

	private Configuration localConfig(Long split_maxsize){
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "file:///");
		conf.set("mapreduce.framework.name", "local");
		conf.set("io.compression.codecs", SPLITTABLE_GZIP);
		conf.set("mapreduce.input.fileinputformat.split.maxsize", split_maxsize.toString());
		return conf;
	}

	private void assertReducedWordCountIsCorrect(InputStream is) throws IOException{
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		Assert.assertEquals(br.readLine(),"This\t10000");
		Assert.assertEquals(br.readLine(),"is\t10000");
		Assert.assertEquals(br.readLine(),"line\t10000");
		br.close();
	}

	@Test
	public void testSplittableGZipCodecInput() {
		try {
			Configuration conf = localConfig(20000L);

			writeThisIsLineToTestfile1(conf);

			wordCountTestfile1To2Job(conf).waitForCompletion(true);

			FileSystem fs = FileSystem.get(conf);
			assertReducedWordCountIsCorrect(fs.open(new Path(TESTFILE_2,"part-r-00000")));
		} catch(Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	@Test
	public void testSplittableGZipCodecOutput() {
		try {
			Configuration conf = localConfig(20000L);
			conf.set("mapreduce.output.fileoutputformat.compress", "true");
			conf.set("mapreduce.output.fileoutputformat.compress.codec", SPLITTABLE_GZIP);
			conf.set(SplittableGZipCodec.NUM_OUTPUT_BYTES_IN_SPLIT, "8");

			writeThisIsLineToTestfile1(conf);

			wordCountTestfile1To2Job(conf).waitForCompletion(true);

			FileSystem fs = FileSystem.get(conf);
			SeekableGZipDataInputStream seekable = new SeekableGZipDataInputStream(fs.open(new Path(TESTFILE_2,"part-r-00000.gz")));
			GZipInputStreamRandomAccess gz = new GZipInputStreamRandomAccess(seekable);
			Map<Long, Long> parts = gz.getMetadata();
			Assert.assertEquals(parts.size(), 3);
			assertReducedWordCountIsCorrect(gz);
		} catch(Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	@AfterMethod
	public void deleteFiles(){
		FileUtil.fullyDelete(new File(TESTFILE_1_STR));
		FileUtil.fullyDelete(new File(TESTFILE_2_STR));
	}
}
