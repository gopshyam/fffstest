package fffstest;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class DataSourceSocket {
	private static final int NUM_STREAMS = 1000;
	private static final int MAX_RECORDS = 10000;
	
	private static final String CONF_NAME = "fs.defaultFS";
	private static final String CONF_VALUE = "hdfs://localhost:9000";
	private static FileSystem fs;
		
	public static void main(String[] args) {
		
		Configuration conf = new Configuration();
		conf.set(CONF_NAME, CONF_VALUE);
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		
		
		List<DataSourceConnection> dsConnList = new ArrayList<DataSourceConnection>();
		List<WriterThread> writerThreadList = new ArrayList<WriterThread>();
		long startTime = 0;
		
		try {
			for (int i = 1; i <= NUM_STREAMS; i++) {
				//Initialize file stuff and threads here.
				//dsConnList.add(new DataSourceConnection(hostName, portNumber, i, fs));
				//int numRecords = dsConnList.get(i-1).writeRecord();
				DataSourceConnection dataSourceConnection = new DataSourceConnection(i, fs);
				writerThreadList.add(new WriterThread(dataSourceConnection, MAX_RECORDS));
				//System.out.println(numRecords);
			}
			
			startTime = System.currentTimeMillis();
			//Start all the threads
			for (WriterThread writerThread : writerThreadList) {
				writerThread.start();
			}
			for (WriterThread writerThread : writerThreadList) {
				writerThread.join();
			}
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		long endTime = System.currentTimeMillis();
		long timeTaken = endTime - startTime;
		System.out.println(String.format("%d Streams and %d records written in %f seconds", NUM_STREAMS, MAX_RECORDS, timeTaken/1000.0f));
	}
}
