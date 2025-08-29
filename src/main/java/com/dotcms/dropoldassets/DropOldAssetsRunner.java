package com.dotcms.dropoldassets;

import com.dotcms.concurrent.DotConcurrentFactory;
import com.dotcms.concurrent.lock.ClusterLockManager;
import com.dotmarketing.business.APILocator;
import com.dotmarketing.common.db.DotConnect;
import com.dotmarketing.db.DbConnectionFactory;
import com.dotmarketing.exception.DotRuntimeException;
import com.dotmarketing.portlets.fileassets.business.FileAssetAPI;
import com.dotmarketing.util.Logger;
import com.liferay.util.FileUtil;
import io.vavr.control.Try;
import java.io.File;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;


public class DropOldAssetsRunner implements Runnable {


  final String SELECT_OLD_CONTENT_INODES = "SELECT c.inode FROM contentlet c"
      + " WHERE "
      + " (c.identifier <> 'SYSTEM_HOST' or c.identifier is null) "
      + " AND c.mod_date >= ? "
      + " AND c.mod_date <= ? "
      + " AND NOT EXISTS "
      + " ("
      + "    SELECT 1 FROM contentlet_version_info vi"
      + "    WHERE "
      + "    vi.working_inode = c.inode "
      + "    OR vi.live_inode = c.inode"
      + " )"
      + " LIMIT ?";
  final String DELETE_CONTENT_DATA = "DELETE FROM contentlet WHERE inode IN (%s)";
  final String DELETE_CONTENT_INODE = "DELETE FROM  inode where inode IN (%s)";
  final String DELETE_TAG_INODES = "DELETE FROM tag_inode where inode IN (%s)";
  final String CREATE_INDEX_CONTENTLET_MOD_DATE = "CREATE INDEX if not exists idx_contentlet_mod_date on contentlet(mod_date)";

  final String EARLIEST_CONTENTLET_DATE = "SELECT min(mod_date) as start_date from contentlet where identifier <> 'SYSTEM_HOST' or identifier is null";



  final boolean DROP_OLD_ASSET_DRY_RUN = Try.of(
      () -> Boolean.parseBoolean(OSGiPluginProperties.getProperty("DROP_OLD_ASSET_DRY_RUN", "true"))).getOrElse(true);


  int olderThanDays = Integer.parseInt(OSGiPluginProperties.getProperty("DROP_OLD_ASSET_OLDER_THAN_DAYS", "30"));
  int batchSize = Integer.parseInt(OSGiPluginProperties.getProperty("DROP_OLD_ASSET_BATCH_SIZE", "100"));
  int daysToIterate = Integer.parseInt(
      OSGiPluginProperties.getProperty("DROP_OLD_ASSET_ITERATE_BY_DAYS", String.valueOf(30)));
  Date finalEndDate = Date.from(Instant.now().minus(olderThanDays, ChronoUnit.DAYS));
  ClusterLockManager<String> locker = DotConcurrentFactory.getInstance()
      .getClusterLockManager(DropOldAssetsRunner.class.getName());

  // For testing purposes
  void insertFakeInode() {
    try (Connection conn = DbConnectionFactory.getConnection()) {
      String uuid = java.util.UUID.randomUUID().toString();
      Logger.info(this, "Inserting fake inode " + uuid);

      conn.setAutoCommit(false);
      DotConnect dc = new DotConnect();
      dc.setSQL("insert into inode (inode, idate, type) values ('" + uuid + "','2018-01-01 00:00:00','contentlet')");
      dc.loadObjectResults(conn);
      dc.setSQL("insert into contentlet (inode, mod_date, contentlet_as_json) values ('" + uuid
          + "', '2018-01-01 00:00:00', '{}')");
      dc.loadObjectResults(conn);
      conn.commit();
      conn.setAutoCommit(true);

    } catch (Exception e) {

      throw new DotRuntimeException(e);
    }


  }

  boolean shouldRun() {

    final String oldestServer = Try.of(() -> APILocator.getServerAPI().getOldestServer()).getOrElse("notIt");
    return (oldestServer.equals(APILocator.getServerAPI().readServerId()));


  }

  Date earliestContentlet()  {
    try (Connection conn = DbConnectionFactory.getConnection()) {

      return Try.of(() -> {
            return (Date) new DotConnect()
                .setSQL(EARLIEST_CONTENTLET_DATE)
                .loadObjectResults(conn)
                .get(0)
                .get("start_date");
          }
      ).getOrElse(Date.from(Instant.parse("2010-01-01T00:00:00Z")));
    } catch (Exception e) {
      throw new DotRuntimeException(e.getMessage(), e);
    }
  }

  List<String> loadInodes(Date startDate, Date endDate, int batchSize, Connection conn) {
    try {
      return new DotConnect()
          .setSQL(SELECT_OLD_CONTENT_INODES)
          .addParam(startDate)
          .addParam(endDate)
          .addParam(batchSize)
          .loadObjectResults(conn)
          .stream()
          .map(row -> (String) row.get("inode"))
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new DotRuntimeException(e);
    }
  }

  void deleteOldContent()  {

    try (Connection conn = DbConnectionFactory.getConnection()) {
      DotConnect dc = new DotConnect();
      Logger.info(this, "Creating index on contentlet.mod_date");
      dc.setSQL(CREATE_INDEX_CONTENTLET_MOD_DATE);
      dc.loadResult(conn);
    }
    catch(Exception e){
      Logger.error(this, "Error creating index on contentlet.mod_date", e);
      throw new DotRuntimeException(e);
    }
    if(isInterrupted()){
      return;
    }
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    insertFakeInode();
    Date startDate = earliestContentlet();
    Date endDate = Date.from(startDate.toInstant().plus(daysToIterate, ChronoUnit.DAYS));
    Logger.info(this, "START: Deleting contentlet older than " + sdf.format(finalEndDate));
    Logger.info(this, "     : earliest contentlet " + sdf.format(startDate));
    int deleted = 0;
    int i = 0;
    final List<String> inodeList = new ArrayList<>();
    while (endDate.getTime() < finalEndDate.getTime()) {
      if(isInterrupted()){
        return;
      }
      Logger.info(this, "Looking for old contentlet from " + sdf.format(startDate) + " to " + sdf.format(endDate));

      try (Connection conn = DbConnectionFactory.getConnection()) {
        inodeList.addAll(loadInodes(startDate, endDate, batchSize, conn));
        while (!inodeList.isEmpty()) {
          if(isInterrupted()){
            return;
          }
          if (DROP_OLD_ASSET_DRY_RUN) {
            Logger.info(this, " --- FOUND  " + inodeList.size() + " old contentlets");
            deleted +=inodeList.size();
            break;
          }
          Logger.info(this, " --- DELETING " + inodeList.size() + " old contentlets");
          conn.setAutoCommit(false);
          String inodes = String.join("','", inodeList);

          inodes = "'" + inodes + "'";
          DotConnect dc = new DotConnect();
          deleted += dc.executeUpdate(conn, String.format(DELETE_CONTENT_DATA, inodes));

          dc.setSQL(String.format(DELETE_CONTENT_INODE, inodes));
          dc.loadResult(conn);

          dc.setSQL(String.format(DELETE_TAG_INODES, inodes));
          dc.loadResult(conn);

          conn.commit();
          conn.setAutoCommit(true);

          deleteFromAssetsDir(inodeList);

          inodeList.clear();
          if(isInterrupted()){
            return;
          }
          inodeList.addAll(loadInodes(startDate, endDate, batchSize, conn));

        }

      } catch (Exception e) {
        Logger.error(this, "Error deleting old content from " + sdf.format(startDate) + " to " + sdf.format(endDate),
            e);

        throw new DotRuntimeException(e);
      }

      inodeList.clear();
      startDate = endDate;
      endDate = Date.from(startDate.toInstant().plus(daysToIterate, ChronoUnit.DAYS));
      endDate = endDate.before(finalEndDate) ? endDate : finalEndDate;

    }
    Logger.info(this, "END: Deleted " + deleted + " old contentlet(s)");

  }

  boolean deleteFromAssetsDir(List<String> inodes) {

    for(String inode : inodes){
      String path = APILocator.getFileAssetAPI().getRealAssetPath(inode).replace(FileAssetAPI.BINARY_FIELD +  File.separator,"");


      Logger.info(this, "Deleting file asset " + path);
      try {
        FileUtil.deltree( path);
      } catch (Exception e) {
        Logger.error(this, "Error deleting file asset " + path, e);
        return false;
      }


    }
    return true;
  }


  boolean isInterrupted() {
    return Thread.currentThread().isInterrupted();

  }


  @Override
  public void run() {

    Logger.info(this, "Delete Drop Old Assets Job Started");
    try {
      locker.tryClusterLock(this::deleteOldContent);
    } catch (Throwable e) {
      Logger.error(this, "Cannot  trying to lock DropOldAssetsRunner", e);

    }

  }
}
