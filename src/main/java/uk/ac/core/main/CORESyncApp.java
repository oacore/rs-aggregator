package uk.ac.core.main;

import nl.knaw.dans.rs.aggregator.schedule.JobScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.FileSystemXmlApplicationContext;
import uk.ac.core.resync.sync.CORESyncJob;

/**
 * Created on 2017-05-04 16:47.
 */
public class CORESyncApp {

  public static final String APP_CONTEXT_LOCATION = "cfg/core-syncapp-context.xml";
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




  private static Logger logger = LoggerFactory.getLogger(CORESyncApp.class);

  public static void main(String[] args) throws Exception {
    logger.info(RS_ART);
    boolean isManualUpdate = true;
    boolean isMeasured = false;
    String uriToDownload = "";
    String appContextLocation;
    int batchSize=0;
    Integer max =0;
    appContextLocation = APP_CONTEXT_LOCATION;
    if (args.length > 0) {
      for (String arg : args) {
        logger.info(arg);
        if (arg.equals("--autoupdate")) {
          isManualUpdate = false;
        } else if (arg.startsWith("--uri=")) {
          String[] parts = arg.split("=");
          uriToDownload = parts[1];
        }
       else if (arg.startsWith("--measure")) {
          isMeasured=true;
        }
        else if (arg.startsWith("--max")) {
          String[] parts = arg.split("=");
          max = Integer.valueOf(parts[1]);
        }
        else {
          appContextLocation = arg;
        }
      }
    }
    logger.info("Configuration file: {}", appContextLocation);

    JobScheduler scheduler;
    CORESyncJob syncJob;
    try (FileSystemXmlApplicationContext applicationContext = new FileSystemXmlApplicationContext(appContextLocation)) {

      scheduler = (JobScheduler) applicationContext.getBean(BN_JOB_SCHEDULER);
      syncJob = (CORESyncJob) applicationContext.getBean(BN_SYNC_JOB);
        syncJob.setUriToDownload(uriToDownload);
        syncJob.setMeasure(isMeasured);
        syncJob.setMaxRecordsToDownload(max);
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
