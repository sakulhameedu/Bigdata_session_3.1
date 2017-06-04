package hdfs;

import java.io.IOException;
import java.io.PrintStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class RecursiveListing
{
  public static void main(String[] args)
  {
    if (args.length != 1) {
      System.out.println("Pass one argument");
      System.exit(1);
    }
    getStatus(new Path(args[0]));
  }

  public static void getStatus(Path path)
  {
    try {
      Configuration conf = new Configuration();
      FileSystem fileSystem = FileSystem.get(path.toUri(), conf);
      FileStatus[] fileStatus = fileSystem.listStatus(path);

      for (FileStatus fStat : fileStatus) {
        if (fStat.isDirectory()) { //checking directory
          System.out.println("Directory: " + fStat.getPath());
          getStatus(fStat.getPath()); //get subdirectory
        }
        else if (fStat.isFile()) { //checking file
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
