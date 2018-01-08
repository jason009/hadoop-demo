package com.jason.hdfs;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

public class HDFSDemo {
	FileSystem fs = null;
	@Before
	public void init() throws Throwable, URISyntaxException {
		fs = FileSystem.get(new URI("hdfs://jason01:9000"), new Configuration(),"root");
	}

	@Test
	public void testUpload() throws RuntimeException, Exception {
		InputStream in = new FileInputStream("D:/learn/JetbrainsCrack-2.6.9-release-enc.jar");
		OutputStream out = fs.create(new Path("/jdk"));
		IOUtils.copyBytes(in, out, 4096, true);
	}
	
	public static void main(String[] args) throws IOException, URISyntaxException {
		FileSystem fs = FileSystem.get(new URI("hdfs://jason01:9000"), new Configuration());
		InputStream in = fs.open(new Path("/words"));
		OutputStream out = new FileOutputStream("d://jdk");
		IOUtils.copyBytes(in, out, 4096, true);
	}

}
