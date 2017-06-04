package hdfs;

import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;

public class RecursiveMultipleListing {
	public static void main(String[] args) {
		if (args.length == 0) {
			System.out.println("Pass at least one argument");
			System.exit(1);
		}
		
		for (String arg : args) {     //passing number of arguments 
			getStatus(new Path(arg));  //passing arguments 
		}
	}
	
	public static void getStatus(Path path) {
		try
		{
			Configuration conf = new Configuration();
			FileSystem fileSystem = FileSystem.get(path.toUri(), conf);
			FileStatus[] fileStatus=fileSystem.listStatus(path);
			
			for (FileStatus fStat : fileStatus) {
				if (fStat.isDirectory()) {
					System.out.println("Directory: " + fStat.getPath());
					getStatus(fStat.getPath()); //getting sub directory
				}
				else if (fStat.isFile()) {
					System.out.println("File: " + fStat.getPath());
				}
				else if (fStat.isSymlink()) {
					System.out.println("Symlink: " + fStat.getPath());
				}
			}

		}
		catch (IOException e)
		{
            e.printStackTrace();
		}
	}
}
