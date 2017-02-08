package fffstest;

import java.io.IOException;

public class WriterThread extends Thread {

	//Thread that writes a number of records using a connection
	
	private DataSourceConnection dataSourceConnection;
	private int maxRecords;
	
	public WriterThread(DataSourceConnection dataSourceConnection, int maxRecords) {
		super();
		this.dataSourceConnection = dataSourceConnection;
		this.maxRecords = maxRecords;
	}
	
	@Override
	public void run() {
		try {
			int numRecords = 0;
			while(numRecords < maxRecords) {
				numRecords = dataSourceConnection.writeRecord();
			}
			//System.out.println(String.format("Writer thread for stream %d has written %d records", dataSourceConnection.getStreamNumber(), numRecords));
			dataSourceConnection.closeConnection();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
