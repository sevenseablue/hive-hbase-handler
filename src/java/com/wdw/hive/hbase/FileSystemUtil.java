package com.wdw.hive.hbase;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.log4j.Logger;

import java.io.IOException;


/**
 * Common utility functions for operating on FileSystem objects.
 *
 */
public class FileSystemUtil {

  private static final Logger LOG = Logger.getLogger(FileSystemUtil.class);

  /**
   * Returns true iff the path is full
   *
   * @param fsuri
   * @return
   */
  public static boolean isHbaseWritableFilesystem(String fsuri) {

    if (fsuri.startsWith("hdfs:") || fsuri.startsWith("viewfs:")) {
      LOG.info("valide file system");
      return true;
    } else
      return false;
  }

  /**
   * return full path
   *
   * @param location
   * @param hookContext
   * @return
   * @throws IOException
   */
  public static String getVlidateHFilePath(String location, HookContext hookContext) throws IOException {
    Path path = new Path(location);
    FileSystem fs = path.getFileSystem(hookContext.getConf());
    String s = fs.getUri().toString();
    if (isHbaseWritableFilesystem(s)) {
      location = s.endsWith("/") ? s.substring(0, s.length() - 1) + path : s + path;
    }
    return location;
  }
}
