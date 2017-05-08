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
import io.gzinga.InputStreamConverter;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Instant;

public class SplittableGZipCodec extends GzipCodec implements SplittableCompressionCodec {

	private static int buf_length = 32 * 1024;

	public static final String NUM_OUTPUT_BYTES_IN_SPLIT = "io.gzinga.hadoop.output.bytes-in-split";
	public static final String NUM_OUTPUT_BYTES_IN_SPLIT_DEFAULT = "39452672"; //32MiB = (32*1024*1024L)
	
	private long getHeader(SeekableGZipDataInputStream in, long loc)  throws IOException {
		long position = loc;
		long newPos = -1;
		while(true) {
			byte[] buf = new byte[buf_length];
			in.seek(position);
			
			int lastIndex = 0;
			int totalLen = 0;
			while(true) {
				int len = in.read(buf, lastIndex, buf_length - lastIndex);
				if(len == -1) {
					break;
				}
				totalLen += len;
				if(totalLen != buf_length) {
						lastIndex = totalLen;
				} else {
					break;
				}
			}
			
			if(totalLen == 0) {
				return position;
			}
			int headerIndex = InputStreamConverter.firstIndexOf(buf, InputStreamConverter.headerbytes, 0, totalLen - 1);
			if(headerIndex == -1) {
				position += (totalLen - InputStreamConverter.headerbytes.length);
			} else {
				newPos = position + headerIndex;
				break;
			}
		}
		return newPos;
	}
	
	@Override
	public SplitCompressionInputStream createInputStream(InputStream arg0,
			Decompressor arg1, long start, long end, READ_MODE arg4)
			throws IOException {
		SeekableGZipDataInputStream sfIn = new SeekableGZipDataInputStream((FSDataInputStream)arg0);
		long newStart = getHeader(sfIn, start);
		long newEnd = getHeader(sfIn, end);
		sfIn.seek(newStart);
		GZipInputStreamRandomAccess gzin = new GZipInputStreamRandomAccess(sfIn, false);		
		return new SplittableGzipInputStream(gzin, newStart, newEnd);
	}

	@Override
	public CompressionOutputStream createOutputStream(OutputStream out){
		long autoOffsetBytes = Long.valueOf(getConf().get(NUM_OUTPUT_BYTES_IN_SPLIT, NUM_OUTPUT_BYTES_IN_SPLIT_DEFAULT));
		GZipOutputStreamRandomAccess gOut = new GZipOutputStreamRandomAccess(out, 32*1024, autoOffsetBytes);
		return new SplittableGzipOutputStream(gOut);
	}

	@Override
	public CompressionOutputStream createOutputStream(OutputStream out, @Nullable Compressor compressor) {
		// Ignore compressor parameter ... it should be null - GZipCodec can do this
		return createOutputStream(out);
	}

	@Override
	public @Nullable Compressor createCompressor() {
		// GZipCodec does this too (when there is no native zlib loaded).
		return null;
	}

	@Override
	public @Nullable Class<? extends Compressor> getCompressorType() {
		// GZipCodec does this too (when there is no native zlib loaded).
		return null;
	}

	private static final class SplittableGzipOutputStream extends CompressionOutputStream{

		public SplittableGzipOutputStream(GZipOutputStreamRandomAccess out) {
			super(out);
		}

		@Override
		public void write(int b) throws IOException {
			out.write(b);
		}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			out.write(b, off, len);
		}

		@Override
		public void finish() throws IOException {
			((GZipOutputStreamRandomAccess)out).finish();
		}

		@Override
		public void resetState() throws IOException {
			// This does more than just reset the deflater because I don't want to take time figuring out what stream
			// metadata needs to be updated to keep the gzip block valid. So, I add an offset based on current time at
			// maximum system precision
			Instant now = Instant.now();
			long nano_secs_since_epoch = (now.getEpochSecond() % (Long.MAX_VALUE/1000000)) * 1000000000L; // overflows every 292 years
			((GZipOutputStreamRandomAccess)out).addOffset(now.getNano() + nano_secs_since_epoch);
		}
	}

	private static final class SplittableGzipInputStream extends SplitCompressionInputStream {

		private long lastRead = -1;
		
		public SplittableGzipInputStream(InputStream in, long start, long end)
				throws IOException {
			super(in, start, end);
			setStart(start);
			setEnd(end);
		}

		@Override
		public void resetState() throws IOException {
			in.reset(); // This is a bug. The underlying stream should not be reset, just the decompressor
		}

		@Override
		public long getPos() throws IOException {
			return lastRead;
		}

		@Override
		public int read(byte[] arg0, int arg1, int arg2) throws IOException {
			lastRead = ((GZipInputStreamRandomAccess)in).getPos();
			return in.read(arg0, arg1, arg2);
		}

		@Override
		public int read() throws IOException {
			lastRead = ((GZipInputStreamRandomAccess)in).getPos();
			return in.read();
		}		
	}
}
