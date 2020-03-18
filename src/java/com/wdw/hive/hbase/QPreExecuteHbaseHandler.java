package com.wdw.hive.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext.HookType;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * When the hfile is successfully generated, it is moved to hbase in batches
 *
 */
public class QPreExecuteHbaseHandler implements ExecuteWithHookContext {

  private static final Logger LOG = Logger.getLogger(FileSystemUtil.class);
  boolean iswrite = false;
  boolean ishivehbasehandler = false;
  LogHelper console = SessionState.getConsole();
  /**
   * Defines the path of hfile temporary directory
   */
  private String hbasetablename = null;
  private String hivetablename = null;

  @Override
  public void run(HookContext hookContext) throws Exception {
    assert (hookContext.getHookType() == HookType.POST_EXEC_HOOK);
    Set<WriteEntity> outputs = hookContext.getOutputs();
    this.run(hookContext, outputs);
  }

  public void run(HookContext hookContext, Set<WriteEntity> outputs) throws Exception {
    if (console == null) {
      return;
    }
    getHiveMetaInfo(outputs);
    if (hivetablename != null && iswrite && ishivehbasehandler) {
      Configuration sessionConf = SessionState.getSessionConf();

      int numParts = 1;
      try {
        numParts = HBaseUtils.getTabKeys(hbasetablename).length;
      } catch (IOException e) {
        e.printStackTrace();
      }
      sessionConf.set("mapred.reduce.tasks", String.valueOf(numParts));
      String partFile = sessionConf.get("mapreduce.totalorderpartitioner.path");
      Map<String, String> vars = SessionState.get().getHiveVariables();
      String partFileWriteAuto = vars.get("mapreduce.totalorderpartitioner.path.auto");
      if (partFileWriteAuto != null && partFileWriteAuto.equals("true")) {
        try {
          HBaseUtils.writePartitionFile(sessionConf, partFile, hbasetablename);
        } catch (IOException e) {
          e.printStackTrace();
        } catch (Throwable throwable) {
          throwable.printStackTrace();
        }
      }
//      sessionConf.set("mapreduce.totalorderpartitioner.path", partFile);
    }
  }


  /**
   * Get hive and hbase table info from meta
   *
   * @param outputs This object may be a table, partition, dfs directory or a local directory.
   */
  public void getHiveMetaInfo(Set<WriteEntity> outputs) throws Exception {

    for (WriteEntity we : outputs) {
      WriteEntity.WriteType writeType = we.getWriteType();
      switch (writeType) {
        case INSERT:
        case INSERT_OVERWRITE: {
          iswrite = true;
          break;
        }

        default:
          break;
      }
    }

    if (iswrite) {
      for (WriteEntity we : outputs) {
        ReadEntity.Type typ = we.getType();
        switch (typ) {
          case TABLE: {
            Table t = we.getTable();
            ishivehbasehandler = isHiveHbaseHandler(t);
            hbasetablename = t.getProperty(Constant.HBASE_TABLE_NAME);
            hivetablename = t.getTableName();
            break;
          }
          default:
            break;
        }
      }
    }
  }


  /**
   * Returns true iff the table is a HiveHBaseHandler
   *
   * @param t
   * @return
   */
  public boolean isHiveHbaseHandler(Table t) {
    return t.getStorageHandler() instanceof HBaseStorageHandler;
  }
}
