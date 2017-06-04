package hdfs;

import java.io.IOException;
import java.io.PrintStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;           //importing apache hadoop configuration
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileListing
{
  public static void main(String[] args)
  {
    if (args.length != 1) {
      System.out.println("Pass one argument");  //pass one argument
      System.exit(1);
    }

    Path path = new Path(args[0]);    //argument passed
    try
    {
      Configuration conf = new Configuration();
      FileSystem fileSystem = FileSystem.get(path.toUri(), conf);
      FileStatus[] fileStatus = fileSystem.listStatus(path);

      for (FileStatus fStat : fileStatus) {  //checking utill all file are read
        if (fStat.isDirectory()) { //checking directory
          System.out.println("Directory: " + fStat.getPath()); //printing
        }
        else if (fStat.isFile()) {//checking file
          System.out.println("File: " + fStat.getPath());  //printing
        }
        else if (fStat.isSymlink()) {
          System.out.println("Symlink: " + fStat.getPath());//printing
        }
      }

    }
    catch (IOException e) //catching exception when no arguments are passed 
    {
      e.printStackTrace();
    }
  }
}
