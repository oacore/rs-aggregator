package uk.ac.core.main;

import nl.knaw.dans.rs.aggregator.schedule.JobScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.FileSystemXmlApplicationContext;
import uk.ac.core.resync.sync.COREBatchSyncJob;

/**
 * Created on 2017-05-04 16:47.
 */
public class COREBatchSyncApp {

  public static final String APP_CONTEXT_LOCATION = "cfg/core-batch-syncapp-context.xml";
  public static final String BN_JOB_SCHEDULER = "job-scheduler";
  public static final String BN_SYNC_JOB = "sync-job";

  private static final String RS_ART = "\n" +
    "_________________________________________________________________________________________________________\n" +
    "     ______  ______       ___     ______  ______   ______   _____  _____    ___________   ______   ______\n" +
    "    / __  / / ____/      /***|   / ___ / / ___ /  / __  |  / ___| / ___/   /   ___  ___| / __   | / __  |\n" +
    "   / /_/ / / /___  ___  /*_**|  / / _   / / _    / /_/ /  / /__  / / _    / _  |  | |   / /  / / / /_/ / \n" +
    "  /  _  | /___  / /__/ /*/_|*| / / | | / / | |  /  _  |  / ___/ / / | |  / /_| |  | |  / /  / / /  _  |  \n" +
    " /  / | | ___/ /      /*___ *|/ /__| |/ /__| | /  / | | / /____/ /__| | / ___  |  | | / /__/ / /  / | |  \n" +
    "/__/  |_|/____/      /_/   |_||______/|______//__/  |_|/______/|______//_/   |_|  |_| |_____/ /__/  |_|  core.ac.uk mod\n" +
    "__________________________________________________________________________________________________________\n";




  private static Logger logger = LoggerFactory.getLogger(COREBatchSyncApp.class);

  public static void main(String[] args) throws Exception {
    logger.info(RS_ART);
    String appContextLocation;
    if (args.length > 0) {
      appContextLocation = args[0];
    } else {
      appContextLocation = APP_CONTEXT_LOCATION;
    }
    boolean isManualUpdate=false;
    if (args.length>1){
      String additionalArg = args[1];
      if (additionalArg.equals("--manual-update")){
        isManualUpdate=true;
      }
    }
    logger.info("Configuration file: {}", appContextLocation);

    JobScheduler scheduler;
    COREBatchSyncJob syncJob;
    try (FileSystemXmlApplicationContext applicationContext = new FileSystemXmlApplicationContext(appContextLocation)) {

      scheduler = (JobScheduler) applicationContext.getBean(BN_JOB_SCHEDULER);
      syncJob = (COREBatchSyncJob) applicationContext.getBean(BN_SYNC_JOB);
      syncJob.setManualUpdate(isManualUpdate);
      applicationContext.close();
    } catch (Exception e) {
      logger.error("Could not configure from {}: ", appContextLocation, e);
      throw e;
    }

    try {
      scheduler.schedule(syncJob);
    } catch (Exception e) {
      logger.error("Last error caught: ", e);
      throw e;
    }
  }

}
