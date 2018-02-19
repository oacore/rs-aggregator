package nl.knaw.dans.rs.aggregator.sync;

import nl.knaw.dans.rs.aggregator.syncore.PathFinder;
import nl.knaw.dans.rs.aggregator.syncore.ResourceManager;
import nl.knaw.dans.rs.aggregator.syncore.Sync;
import nl.knaw.dans.rs.aggregator.syncore.VerificationPolicy;
import nl.knaw.dans.rs.aggregator.syncore.VerificationStatus;
import nl.knaw.dans.rs.aggregator.util.RsProperties;
import nl.knaw.dans.rs.aggregator.xml.RsConstants;
import nl.knaw.dans.rs.aggregator.xml.RsMd;
import nl.knaw.dans.rs.aggregator.xml.UrlItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Optional;

/**
 * Created on 2017-04-15 14:36.
 */
public class SyncWorker implements RsConstants {

  private final static Logger logger = LoggerFactory.getLogger(SyncWorker.class);

  private static final int MAX_DOWNLOADS = Integer.MAX_VALUE;
  private static final int MAX_DOWNLOAD_RETRY = 3;

  protected SitemapCollector sitemapCollector;
  protected ResourceManager resourceManager;
  protected VerificationPolicy verificationPolicy;

  protected int maxDownloads = MAX_DOWNLOADS;
  protected int maxDownloadRetry = MAX_DOWNLOAD_RETRY;
  protected boolean trialRun = false;

  protected int itemCount;
  protected int verifiedItems;
  protected int itemsDeleted;
  protected int itemsCreated;
  protected int itemsUpdated;
  protected int itemsRemain;
  protected int itemsNoAction; // change='deleted' and resource does not exists.
  protected int failedDeletions;
  protected int failedCreations;
  protected int failedUpdates;
  protected int failedRemains;
  protected int totalFailures;

  protected int downloadCount;
  protected boolean syncComplete;

  protected int preventedActions;

  public SyncWorker() {

  }

  public SitemapCollector getSitemapCollector() {
    if (sitemapCollector == null) {
      sitemapCollector = new SitemapCollector();
    }
    return sitemapCollector;
  }

  public SyncWorker withSitemapCollector(SitemapCollector sitemapCollector) {
    this.sitemapCollector = sitemapCollector;
    return this;
  }

  public ResourceManager getResourceManager() {
    if (resourceManager == null) {
      resourceManager = new FsResourceManager();
    }
    return resourceManager;
  }

  public SyncWorker withResourceManager(ResourceManager resourceManager) {
    this.resourceManager = resourceManager;
    return this;
  }

  public VerificationPolicy getVerificationPolicy() {
    if (verificationPolicy == null) {
      verificationPolicy = new DefaultVerificationPolicy();
    }
    return verificationPolicy;
  }

  public SyncWorker withVerificationPolicy(VerificationPolicy verificationPolicy) {
    this.verificationPolicy = verificationPolicy;
    return this;
  }

  public int getMaxDownloads() {
    return maxDownloads;
  }

  public SyncWorker withMaxDownloads(int maxDownloads) {
    this.maxDownloads = maxDownloads;
    return this;
  }

  public int getMaxDownloadRetry() {
    return maxDownloadRetry;
  }

  public SyncWorker withMaxDownloadRetry(int maxDownloadRetry) {
    this.maxDownloadRetry = maxDownloadRetry;
    return this;
  }

  public boolean isTrialRun() {
    return trialRun;
  }

  public SyncWorker withTrialRun(boolean trialRun) {
    this.trialRun = trialRun;
    return this;
  }

  public void synchronize(PathFinder pathFinder, RsProperties syncProps) {
    reset();
    getResourceManager().setPathFinder(pathFinder);
    syncLocalResources(pathFinder, syncProps);
    report(pathFinder, syncProps);
  }

  private void reset() {
    itemCount = 0;
    verifiedItems = 0;

    itemsCreated = 0;
    itemsUpdated = 0;
    itemsRemain = 0;
    itemsDeleted = 0;

    itemsNoAction = 0;

    failedCreations = 0;
    failedUpdates = 0;
    failedRemains = 0;
    failedDeletions = 0;
    totalFailures = 0;

    downloadCount = 0;

    preventedActions = 0;
    syncComplete = false;
  }

  protected void syncLocalResources(PathFinder pathFinder, RsProperties syncProps) {
    SitemapCollector collector = getSitemapCollector();
    collector.collectSitemaps(pathFinder, syncProps);
    if (collector.hasErrors()) {
      logger.warn("Not synchronizing because of previous {} errors: {}",
        collector.countErrors(), pathFinder.getCapabilityListUri());
    } else {
      if (collector.hasNewResourceList() && !trialRun) {
        resourceManager.keepOnly(collector.getMostRecentItems().keySet());
      }
      for (Map.Entry<URI, UrlItem> entry : collector.getMostRecentItems().entrySet()) {
        syncItem(entry.getKey(), entry.getValue());
      }
    }
    totalFailures = failedCreations + failedUpdates + failedDeletions + failedRemains;

    syncComplete = !trialRun && !collector.hasErrors() && preventedActions == 0 && totalFailures == 0;

    logger.info("====> synchronized={}, new ResourceList={}, items={}, verified={}, " +
        "failures={}, downloads={} [success/failures] " +
        "created={}/{}, updated={}/{}, remain={}/{}, deleted={}/{}, " +
        "no_action={}, trial run={}, resource set={}",
      syncComplete, collector.hasNewResourceList(), itemCount, verifiedItems, totalFailures, downloadCount, itemsCreated,
      failedCreations, itemsUpdated,
      failedUpdates, itemsRemain, failedRemains,
      itemsDeleted, failedDeletions, itemsNoAction, trialRun, pathFinder.getCapabilityListUri());
  }

  protected void syncItem(URI normalizedURI, UrlItem item) {
    itemCount++;
    String change = item.getMetadata().flatMap(RsMd::getChange).orElse(CH_REMAIN);
    boolean resourceExists = resourceManager.exists(normalizedURI);

    logger.debug("------> {} {}, exists={}, normalizedURI={}", itemCount, change, resourceExists, normalizedURI);

    if (CH_REMAIN.equalsIgnoreCase(change)) {
      if (verifyChange(normalizedURI, item, resourceExists)) {
        itemsRemain++;
      } else {
        failedRemains++;
      }
    } else if (CH_CREATED.equalsIgnoreCase(change)) {
      if (verifyChange(normalizedURI, item, resourceExists)) {
        itemsCreated++;
      } else {
        failedCreations++;
      }
    } else if (CH_UPDATED.equalsIgnoreCase(change)) {
      if (verifyChange(normalizedURI, item, resourceExists)) {
        itemsUpdated++;
      } else {
        failedUpdates++;
      }
    } else if (CH_DELETED.equalsIgnoreCase(change) && resourceExists) {
      if (actionAllowed(normalizedURI) && resourceManager.delete(normalizedURI)) {
        itemsDeleted++;
      } else {
        failedDeletions++;
      }
    } else if (CH_DELETED.equalsIgnoreCase(change) && !resourceExists) {
      itemsNoAction++;
    }
  }

  protected boolean verifyChange(URI normalizedURI, UrlItem item, boolean resourceExists) {
    boolean success;
    if (resourceExists) {
      boolean verified = doVerify(normalizedURI, item);
      if (verified) {
        success = actionAllowed(normalizedURI) && resourceManager.keep(normalizedURI);
      } else {
        success = actionAllowed(normalizedURI) && resourceManager.update(normalizedURI)
          && verifyAndUpdate(normalizedURI, item);
        if (success) downloadCount++;
      }
    } else { // resource does not exist
      success = actionAllowed(normalizedURI) && resourceManager.create(normalizedURI)
        && verifyAndUpdate(normalizedURI, item);
      if (success) downloadCount++;
    }
    return success;
  }

  private boolean verifyAndUpdate(URI normalizedURI, UrlItem item) {
    boolean verified = false;
    for (int i=0; i < getMaxDownloadRetry(); i++) {
      verified = doVerify(normalizedURI, item);
      if (verified) {
        break;
      } else {
        if (!actionAllowed(normalizedURI)) {
          break;
        } else {
          logger.info("Repeating download. download count={}, uri={}", i, normalizedURI);
          resourceManager.update(normalizedURI);
        }
      }
    }
    return verified;
  }

  protected boolean actionAllowed(URI normalizedURI) {
    boolean allowed = true;
    if (trialRun) {
      logger.debug("Trial run. No action on: {}", normalizedURI);
      allowed = false;
      preventedActions++;
    } else if (downloadCount >= maxDownloads) {
      logger.debug("Max downloads reached. No further action on: {}", normalizedURI);
      allowed = false;
      preventedActions++;
    }
    return allowed;
  }

  protected boolean doVerify(URI normalizedURI, UrlItem item) {
    VerificationPolicy policy = getVerificationPolicy();
    VerificationStatus stHash = VerificationStatus.not_verified;
    VerificationStatus stLastMod = VerificationStatus.not_verified;
    VerificationStatus stSize = VerificationStatus.not_verified;

    if (policy.continueVerification(stHash, stLastMod, stSize)) {
      Optional<String> maybeHash = item.getMetadata().flatMap(RsMd::getHash);
      if (maybeHash.isPresent()) {
        String hash = maybeHash.get();
        String algorithm = "md5";
        String[] splitHash = hash.split(":");
        if (splitHash.length > 1) {
          algorithm = splitHash[0];
          hash = splitHash[1];
        }
        stHash = resourceManager.verifyHash(normalizedURI, algorithm, hash);
      }
    }

    if (policy.continueVerification(stHash, stLastMod, stSize)) {
      Optional<ZonedDateTime> maybeLastModified = item.getLastmod();
      if (maybeLastModified.isPresent()) {
        stLastMod = resourceManager.verifyLastModified(normalizedURI, maybeLastModified.get());
      }
    }

    if (policy.continueVerification(stHash, stLastMod, stSize)) {
      Optional<Long> maybeSize = item.getMetadata().flatMap(RsMd::getLength);
      if (maybeSize.isPresent()) {
        stSize = resourceManager.verifySize(normalizedURI, maybeSize.get());
      }
    }

    if (policy.repeatDownload(stHash, stLastMod, stSize)) {
      boolean success = actionAllowed(normalizedURI) && resourceManager.update(normalizedURI);
      if (success) downloadCount++;
    }

    boolean verified = policy.isVerified(stHash, stLastMod, stSize);
    if (verified) verifiedItems++;
    logger.debug("Verification status={}, Hash={}, LastMod={}, Size={}, uri={}",
      verified, stHash, stLastMod, stSize, normalizedURI);
    return verified;
  }

  private void report(PathFinder pathFinder, RsProperties syncProps) {
    ZonedDateTime syncEnd = ZonedDateTime.now().withZoneSameInstant(ZoneOffset.UTC);

    syncProps.setDateTime(Sync.PROP_SW_SYNC_START, pathFinder.getSyncStart());
    syncProps.setDateTime(Sync.PROP_SW_SYNC_END, syncEnd);
    syncProps.setBool(Sync.PROP_SW_FULLY_SYNCHRONIZED, syncComplete);

    syncProps.setInt(Sync.PROP_SW_MAX_DOWNLOADS, getMaxDownloads());
    syncProps.setInt(Sync.PROP_SW_MAX_DOWNLOAD_RETRY, getMaxDownloadRetry());
    syncProps.setBool(Sync.PROP_SW_TRIAL_RUN, isTrialRun());
    syncProps.setProperty(Sync.PROP_SW_SITEMAP_COLLECTOR, getSitemapCollector().getClass().getName());
    syncProps.setProperty(Sync.PROP_SW_RESOURCE_MANAGER, getResourceManager().getClass().getName());
    syncProps.setProperty(Sync.PROP_SW_VERIFICATION_POLICY, getVerificationPolicy().getClass().getName());

    syncProps.setInt(Sync.PROP_SW_TOTAL_ITEMS, itemCount);
    syncProps.setInt(Sync.PROP_SW_ITEMS_VERIFIED, verifiedItems);
    syncProps.setInt(Sync.PROP_SW_ITEMS_DELETED, itemsDeleted);
    syncProps.setInt(Sync.PROP_SW_ITEMS_CREATED, itemsCreated);
    syncProps.setInt(Sync.PROP_SW_ITEMS_UPDATED, itemsUpdated);
    syncProps.setInt(Sync.PROP_SW_ITEMS_REMAIN, itemsRemain);
    syncProps.setInt(Sync.PROP_SW_ITEMS_NO_ACTION, itemsNoAction);
    syncProps.setInt(Sync.PROP_SW_TOTAL_FAILED_ITEMS, totalFailures);
    syncProps.setInt(Sync.PROP_SW_FAILED_DELETIONS, failedDeletions);
    syncProps.setInt(Sync.PROP_SW_FAILED_CREATIONS, failedCreations);
    syncProps.setInt(Sync.PROP_SW_FAILED_UPDATES, failedUpdates);
    syncProps.setInt(Sync.PROP_SW_FAILED_REMAINS, failedRemains);

    syncProps.setInt(Sync.PROP_SW_TOTAL_DOWNLOAD_COUNT, downloadCount);

    try {
      File file = pathFinder.getSyncPropXmlFile();
      String lsb = "Last saved by " + this.getClass().getName();
      syncProps.storeToXML(file, lsb);
      logger.debug("Saved SyncWorker properties to {}", file);
    } catch (IOException e) {
      logger.error("Could not save syncProps", e);
      throw new RuntimeException(e);
    }
  }

}
