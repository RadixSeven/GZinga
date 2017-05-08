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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import io.gzinga.GZipInputStreamRandomAccess;
import io.gzinga.GZipOutputStreamRandomAccess;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.util.ArrayList;

public class TestHadoopGZipRandomAccess {

	@Test
	public void testGZipOutputStream() {
		ArrayList<Path> to_delete = new ArrayList<>();
		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "file:///");
			FileSystem fs = FileSystem.get(conf);
			Path test_dir = new Path("target/test");
			Path test_file = new Path("target/test/testfile");
			Path test_file1 = new Path("target/test/testfile1");
			fs.mkdirs(test_dir);
			to_delete.add(test_dir);
			GZipOutputStreamRandomAccess gzip = new GZipOutputStreamRandomAccess(
					fs.create(test_file));
			to_delete.add(test_file);
			byte[] str = "This is line\n".getBytes();
			for(int i = 1; i <= 10000; i++) {
				if(i % 100 == 0) {
					gzip.addOffset(i/ 100L);
				}
				gzip.write(str);
			}
			Assert.assertEquals(gzip.getOffsetMap().size(), 100);
			gzip.close();
			fs.copyFromLocalFile(new Path(fs.getWorkingDirectory().toString() + "/target/test-classes/testfile1"),
					test_file1);
			to_delete.add(test_file1);

			FSDataInputStream fin = fs.open(test_file);
			long len = fs.getFileStatus(test_file).getLen();
			SeekableGZipDataInputStream sin = new SeekableGZipDataInputStream(fin, len);
			Assert.assertTrue(GZipInputStreamRandomAccess.isGzipRandomOutputFile(sin));

			fin = fs.open(test_file1);
			sin = new SeekableGZipDataInputStream(fin, len);
			Assert.assertFalse(GZipInputStreamRandomAccess.isGzipRandomOutputFile(sin));

			fin = fs.open(test_file);
			sin = new SeekableGZipDataInputStream(fin, len);
			GZipInputStreamRandomAccess gzin = new GZipInputStreamRandomAccess(sin);
			Assert.assertEquals(gzin.getMetadata().size(), 100);
			Assert.assertTrue(gzin.getMetadata().containsKey(1L));
			Assert.assertTrue(gzin.getMetadata().containsKey(100L));
			Assert.assertFalse(gzin.getMetadata().containsKey(200L));
			gzin.jumpToIndex(50L);
			int count1 = 0;
			while(true) {
				int l = gzin.read();
				if(l == -1) {
					break;
				}
				count1++;
			}
			gzin.jumpToIndex(60L);
			int count2 = 0;
			while(true) {
				int l = gzin.read();
				if(l == -1) {
					break;
				}
				count2++;
			}
			Assert.assertTrue(count1 > count2);
			gzin.close();
		} catch(Exception e) {
			e.printStackTrace();
			Assert.fail();
		} finally {
			for(Path f: to_delete){
				FileUtil.fullyDelete(new File(f.getName()));
			}
		}
	}
}
