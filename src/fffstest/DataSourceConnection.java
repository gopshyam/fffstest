package fffstest;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DataSourceConnection {
	private int streamNumber;
	private Socket dataSourceSocket;
	private PrintWriter out;
	private InputStream in;
	private FSDataOutputStream fsos;
	private DataOutputStream dos;
	private static final String OUTPUT_FILE_FORMAT = "/testfiles/stream_%d";
	private static final int TIME_OFFSET = 6;
	private static final int TIME_LENGTH = 4;
	private static final int FRAC_OFFSET = 10;
	private static final int FRAC_LENGTH = 4;
	
	private int recordsWritten = 0;
	
	public DataSourceConnection(String hostName, int portNumber, int stream_number, FileSystem fs) throws UnknownHostException, IOException {
		this.streamNumber = stream_number;
		Path outputFilePath = new Path(String.format(OUTPUT_FILE_FORMAT, streamNumber));
		if (fs.exists(outputFilePath)) {
			fs.delete(outputFilePath, true);
		}
		fsos = fs.create(outputFilePath);
		dos = new DataOutputStream(fsos);
		dataSourceSocket = new Socket(hostName, portNumber);
		out = new PrintWriter(dataSourceSocket.getOutputStream(), true);
		in = dataSourceSocket.getInputStream();
		String requestString = String.format("%d\n\0", streamNumber);
		out.println(requestString);
	}
	
	public void closeConnection() throws IOException {
		fsos.close();
		dos.close();
		in.close();
		out.close();
		dataSourceSocket.close();
	}
	
	public void writeLine(byte[] frame) throws IOException {
		dos.write(frame);
		dos.writeChar('\n');
	}
	
	public int readData(byte[] data) throws IOException {
		return in.read(data);
	}
	
	public int writeRecord() throws IOException {
		byte[] frame = new byte[200];
		readData(frame);
		long timestamp = getTimeStamp(frame);
		addTimestamp(timestamp, frame);
		writeLine(frame);
		recordsWritten += 1;
		return recordsWritten;
	}
	
	private static long findField(byte[] b, int offset, int length){
		String frameSizeHex = "0x";
		for(int i = 0; i < length; i++){
			frameSizeHex += Integer.toHexString(0x000000ff & b[offset + i]);
		}
		return Long.decode(frameSizeHex).longValue();
	}
	
	private static long getTimeStamp(byte[] frame) {
		return getJavaTime(findField(frame, TIME_OFFSET, TIME_LENGTH), findField(frame, FRAC_OFFSET, FRAC_LENGTH), 100000);
	}
	
	public static long getJavaTime(long soc, long frac, int timeBase) // SOC*1000 to set the seconds to millisecond correctly, then add the fraction of a second. Only the bottom 32bits from each argument are used
    {      
        return (0x00000000FFFFFFFFl&soc)*1000l+(long)fracToMS(frac, timeBase);
    }
	
	public static int fracToMS(long time, int timeBase) //returns fraction of a second in milliseconds. Only the bottom 24bits from the long are used.
    {       //mask out the upper byte (c37 flags) &0x00ffffff
        return Math.round( ((float)(time&0x0000000000FFFFFFl)/(float)(0x0000000000FFFFFFl&timeBase)) * 1000f );
    }
	
	private static void addTimestamp(long ts,byte[] buf){
	    StringBuffer sb = new StringBuffer();
	    sb.append(ts);
	    sb.append(" ");
	    byte bs[] = sb.toString().getBytes();
	    System.arraycopy(bs,0,buf,0,bs.length);
	  }
	
	public int getStreamNumber() {
		return streamNumber;
	}


}
