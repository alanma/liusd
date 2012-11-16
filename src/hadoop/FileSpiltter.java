package hadoop;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * @author liusheding
 * @version 1.0
 * @create_date 2012-9-19
 */
public class FileSpiltter {

	public static final int MAX_LINE = 1000000;
	
	public FileSystem fs;

	public FileSpiltter() throws IOException {
		Configuration conf = HBaseConfiguration.create();
		fs = FileSystem.get(conf);
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		FileSpiltter spiltter = new FileSpiltter();
		spiltter.split(
				"hdfs://master.hadoop:9000/user/hadoop/cups/CUPS_DETAIL",
				"hdfs://master.hadoop:9000/user/hadoop/cups1/1.txt");
	}

	public void split(String src, String dest) throws IOException {

		InputStream in = fs.open(new Path(src));
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		BufferedWriter writer = null;
		FSDataOutputStream out = fs.create(new Path(dest));

		writer = new BufferedWriter(new OutputStreamWriter(out));

		String line = reader.readLine();
		int i = 0;
		while (i < MAX_LINE && line != null) {
			writer.write(line + "\n");
			line = reader.readLine();
			i++;
		}
		in.close();
		reader.close();
		out.close();
		writer.close();
	}
}
