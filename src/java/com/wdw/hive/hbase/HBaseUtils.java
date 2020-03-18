package com.wdw.hive.hbase;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.snapshot.SnapshotDoesNotExistException;
import org.apache.hadoop.hbase.snapshot.SnapshotExistsException;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Properties;


public class HBaseUtils {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseUtils.class);

  private static String HADOOP_HOME = System.getenv("HADOOP_HOME");
  private static String HBASE_HOME = System.getenv("HBASE_HOME");

  public static Configuration getConf() {
    Configuration conf = null;
    conf = HBaseConfiguration.create();
    conf.set("fs.viewfs.impl", org.apache.hadoop.fs.viewfs.ViewFileSystem.class.getName());
    conf.addResource(new Path(HADOOP_HOME + "/etc/hadoop/core-site.xml"));
    conf.addResource(new Path(HADOOP_HOME + "/etc/hadoop/hdfs-site.xml"));
    conf.addResource(new Path(HADOOP_HOME + "/etc/hadoop/mountTable.xml"));
    conf.addResource(new Path(HBASE_HOME + "/conf/hbase-site.xml"));
    return conf;
  }

  public static byte[][] getTabKeys(String tabName) throws IOException {
    return getTabKeys(getConf(), tabName);
  }

  public static byte[][] getTabKeys(Configuration conf, String tabName) throws IOException {
    Configuration config = HBaseConfiguration.create(conf);
    HTable hTable = new HTable(config, tabName);
    byte[][] startKeys = hTable.getRegionLocator().getStartKeys();
    hTable.close();
    return startKeys;
  }

  public static int getRegionNum(Configuration conf, String tabName) throws IOException {
    return getTabKeys(conf, tabName).length;
  }

  public static void delIfExistHfile(Configuration conf, String file) throws IOException {
    String hfilePath = conf.get(Constant.HFILE_FAMILY_PATH);
    if (hfilePath != null && hfilePath.length() > 5 && file.contains(hfilePath)) {
      Path dst = new Path(file);
      FileSystem fs = dst.getFileSystem(conf);
      if (fs.exists(dst)) {
        fs.delete(dst, false);
      }
    }
  }

  public static AbstractSerDe getSerDe(String fieldNames, String fieldTypes, String order, String nullOrder)
      throws Throwable {
    Properties schema = new Properties();
    schema.setProperty(serdeConstants.LIST_COLUMNS, fieldNames);
    schema.setProperty(serdeConstants.LIST_COLUMN_TYPES, fieldTypes);
    schema.setProperty(serdeConstants.SERIALIZATION_SORT_ORDER, order);
    schema.setProperty(serdeConstants.SERIALIZATION_NULL_SORT_ORDER, nullOrder);

    BinarySortableSerDe serde = new BinarySortableSerDe();
    SerDeUtils.initializeSerDe(serde, SessionState.get().getConf(), schema, null);
    return serde;
  }

  public static BytesWritable getBinarySort(RowKey startKey) throws Throwable {

    StructObjectInspector rowOI = (StructObjectInspector) ObjectInspectorFactory
        .getReflectionObjectInspector(RowKey.class,
            ObjectInspectorFactory.ObjectInspectorOptions.JAVA);

    String fieldNames = ObjectInspectorUtils.getFieldNames(rowOI);
    String fieldTypes = ObjectInspectorUtils.getFieldTypes(rowOI);

    String order;
    order = StringUtils.leftPad("", RowKey.fieldCount, '+');
    String nullOrder;
    nullOrder = StringUtils.leftPad("", RowKey.fieldCount, 'a');

    AbstractSerDe serde = getSerDe(fieldNames, fieldTypes,
        order, nullOrder);
    BytesWritable s = (BytesWritable) serde.serialize(startKey, rowOI);

    return s;
  }

  public static void writePartitionFile(Configuration conf, String partitionFile, String tabName)
      throws Throwable {
    byte[][] keys = getTabKeys(conf, tabName);
    Path dst = new Path(partitionFile);
    FileSystem fs = dst.getFileSystem(conf);
    if (fs.exists(dst)) {
      fs.delete(dst, false);
    }

//    SequenceFile.Writer writer = SequenceFile.createWriter(fs,
//        conf, dst, HiveKey.class, NullWritable.class);
//    SequenceFile.Writer writer = SequenceFile.createWriter(new Configuration(),
//        SequenceFile.Writer.file(dst),
//        SequenceFile.Writer.keyClass(HiveKey.class),
//        SequenceFile.Writer.valueClass(NullWritable.class),
//        SequenceFile.Writer.compression(SequenceFile.CompressionType.RECORD, new GzipCodec()));

    SequenceFile.Writer writer = SequenceFile.createWriter(new Configuration(),
        SequenceFile.Writer.file(dst),
        SequenceFile.Writer.keyClass(HiveKey.class),
        SequenceFile.Writer.valueClass(NullWritable.class));

    NullWritable nullValue = NullWritable.get();
    for (int i = 1; i < keys.length; ++i) {
      HiveKey hiveKey = new HiveKey();
      BytesWritable bw = getBinarySort(new RowKey(keys[i]));
      hiveKey.set(bw);
      writer.append(hiveKey, nullValue);
//      writer.append(new HiveKey(keys[i], keys[i].hashCode()), nullValue);
    }
    writer.close();
  }

  public static void createSnapshot(String snapshotname, String name) throws IOException {
    createSnapshot(snapshotname, name, getConf());
  }

  public static void createSnapshot(String snapshotName, String name, Configuration conf) {
    TableName tableName = TableName.valueOf(name);
    Connection conn = null;
    Admin admin = null;
    SessionState.LogHelper console = SessionState.getConsole();
    try {
      conn = ConnectionFactory.createConnection(conf);
      admin = conn.getAdmin();
      if (admin.tableExists(tableName)) {
        admin.snapshot(snapshotName, tableName);
        console.printInfo("snapshot " + snapshotName + " created");
      } else {
        console.printInfo("hbase table " + name + " not exists!");
      }

    } catch (SnapshotExistsException e) {
      console.printInfo("snapshot " + snapshotName + " already exist");
      console.printInfo("snapshot " + snapshotName + " deleting");
      try {
        admin.deleteSnapshot(snapshotName);
        console.printInfo("snapshot " + snapshotName + " deleted");
        admin.snapshot(snapshotName, tableName);
        console.printInfo("snapshot " + snapshotName + " created");
      } catch (IOException ex) {
        ex.printStackTrace();
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (admin != null) {
        try {
          admin.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      if (conn != null) {
        try {
          conn.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  public static void deleteSnapshot(String snapshotName, Configuration conf) {
    Connection conn = null;
    Admin admin = null;
    SessionState.LogHelper console = SessionState.getConsole();
    try {
      conn = ConnectionFactory.createConnection(conf);
      admin = conn.getAdmin();
      admin.deleteSnapshot(snapshotName);
      console.printInfo("snapshot " + snapshotName + " deleted");
    } catch (SnapshotDoesNotExistException e) {
      console.printInfo("snapshot " + snapshotName + " does not exist");
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (admin != null) {
        try {
          admin.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      if (conn != null) {
        try {
          conn.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  public static void checkConfArgument(boolean exp, @Nullable Object errorMessage) {
    if (exp) {
      throw new NullPointerException(String.valueOf(errorMessage));
    }
  }

  /**
   * Generate the hfile unique directory from the hbase table
   *
   * @param hfileprepath
   * @param hbaseName
   * @return
   */
  public static String getHfilePath(String hfileprepath, String hbaseName) {
    hbaseName = hbaseName.contains(":") ? hbaseName.replace(":", "/") : Constant.HBASE_DEFAULT_PRE + hbaseName;
    String hfilepath = null;
    if (!hfileprepath.endsWith(Constant.PATH_DELIMITER)) {
      hfilepath = hfileprepath + Constant.PATH_DELIMITER + hbaseName;
    } else {
      hfilepath = hfileprepath + hbaseName;
    }
    return hfilepath;
  }

  public static void setConf(Configuration jc, String name, String value) {
    jc.set(name, value);
  }

  /**
   * Check whether the parameters are configured,Must be configured "hfile.family.name",Otherwise it can't query
   *
   * @param jc         job configuration
   * @param tableProps
   * @return
   */
  public static String getHfileFamilyPath(Configuration jc, Properties tableProps) {
    String hfileprepath = jc.get(Constant.HFILE_FAMILY_PATH, tableProps.getProperty(Constant.HFILE_FAMILY_PATH));
    checkConfArgument(
        hfileprepath == null,
        "Please set " + Constant.HFILE_FAMILY_PATH + " to target location for HFiles");
    String hbaseName = tableProps.getProperty(Constant.HBASE_TABLE_NAME);
    checkConfArgument(
        hbaseName == null,
        "Please set " + Constant.HBASE_TABLE_NAME + " to target hbase table");
    String familyName = tableProps.getProperty(Constant.HBASE_TABLE_FAMILY_NAME);
    checkConfArgument(
        familyName == null,
        "Please set " + Constant.HBASE_TABLE_FAMILY_NAME + " to target hbase table family name");

    String hfilePath = getHfilePath(hfileprepath, hbaseName);
    setConf(jc, Constant.BULKLOAD_HFILE_PATH, hfilePath);

    return hfilePath + Constant.PATH_DELIMITER + familyName;
  }

  /**
   * Set the temporary directory of the hfile intermediate file
   *
   * @param jc
   * @param tableProps
   * @return
   */
  public static String getFinalOutPath(Configuration jc, Properties tableProps) {
    String hfilePrePath = jc.get(Constant.HFILE_FAMILY_PATH, tableProps.getProperty(Constant.HFILE_FAMILY_PATH));
    String hbaseName = tableProps.getProperty(Constant.HBASE_TABLE_NAME);
    String familyName = tableProps.getProperty(Constant.HBASE_TABLE_FAMILY_NAME);
    return getHfilePath(hfilePrePath + "/__hfile_out_tmp/", hbaseName + "_" + familyName);
  }

  public static String getPartitionFilePath(Configuration jc, Properties tableProps) {
    String hfilePrePath = jc.get(Constant.HFILE_FAMILY_PATH, tableProps.getProperty(Constant.HFILE_FAMILY_PATH));
    String hbaseName = tableProps.getProperty(Constant.HBASE_TABLE_NAME);
    String familyName = tableProps.getProperty(Constant.HBASE_TABLE_FAMILY_NAME);
    return getHfilePath(hfilePrePath + "/__partition_file_path__/", hbaseName + "_" + familyName + "/000000_0");
  }

  public static void main(String[] args) throws Throwable {
    String partsFile = args[1];
    String tableName = args[0];
    HBaseUtils.writePartitionFile(HBaseUtils.getConf(), partsFile, tableName);
  }

  static class RowKey {
    public final static int fieldCount = 1;
    public byte[] startKey;

    public RowKey(byte[] startKey) {
      this.startKey = startKey;
    }

    public byte[] getStartKey() {
      return startKey;
    }

    public void setStartKey(byte[] startKey) {
      this.startKey = startKey;
    }
  }
}
