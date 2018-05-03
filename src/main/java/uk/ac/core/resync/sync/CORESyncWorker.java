package uk.ac.core.resync.sync;

import nl.knaw.dans.rs.aggregator.http.Result;
import nl.knaw.dans.rs.aggregator.sync.SyncWorker;
import nl.knaw.dans.rs.aggregator.syncore.PathFinder;
import nl.knaw.dans.rs.aggregator.util.RsProperties;
import nl.knaw.dans.rs.aggregator.xml.RsRoot;
import nl.knaw.dans.rs.aggregator.xml.UrlItem;
import nl.knaw.dans.rs.aggregator.xml.Urlset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.core.resync.syncore.COREResourceManager;

/**
 * Created by mc26486 on 15/01/2018.
 */
public class CORESyncWorker extends SyncWorker implements SitemapDownloadedListener {

    private final static Logger logger = LoggerFactory.getLogger(CORESyncWorker.class);
    private CORESitemapCollector collector;
    private PathFinder pathFinder;

    public CORESyncWorker() {
        super();
    }

    @Override
    protected void syncLocalResources(PathFinder pathFinder, RsProperties syncProps) {
        this.pathFinder = pathFinder;
        this.collector = getCORESitemapCollector();
        collector.collectSitemaps(pathFinder, syncProps, this);
        if (collector.hasErrors()) {
            logger.warn("Not synchronizing because of previous {} errors: {}",
                    collector.countErrors(), pathFinder.getCapabilityListUri());
        } else {
            if (collector.hasNewResourceList() && !trialRun) {
                resourceManager.keepOnly(collector.getMostRecentItems().keySet());
            }
            //this.batches(new LinkedList<>(collector.getMostRecentItems().entrySet()), COREAPI_BATCH_SIZE).forEach(e -> syncBatchItems(e));


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


    public void sitemapDownloaded(Result<RsRoot> result) {
        if (result.hasErrors()) {
            collector.getErrorResults().add(result);
            for (Throwable error : result.getErrors()) {
                logger.warn("Result has errors. URI: {}, msg: {}", pathFinder.getCapabilityListUri(), error.getMessage());
            }
        } else {
            collector.analyze(result);
            logger.info("analyzed " + result.getUri().toASCIIString());
            if (result.getContent().isPresent()) {
                Object content = result.getContent().get();
                if (content instanceof Urlset) {
                    Urlset urlSet = (Urlset) content;
                    urlSet.getItemList().stream().forEach(e -> this.executeDownload(e));

                }

            }
        }
        collector.flushRecentItems();

    }

    private void executeDownload(UrlItem urlItem) {

        syncItem(urlItem.getNormalizedUri().get(), urlItem);
        logger.info("====> synchronized={}, new ResourceList={}, items={}, verified={}, " +
                        "failures={}, downloads={} [success/failures] " +
                        "created={}/{}, updated={}/{}, remain={}/{}, deleted={}/{}, " +
                        "no_action={}, trial run={}, resource set={}",
                syncComplete, collector.hasNewResourceList(), itemCount, verifiedItems, totalFailures, downloadCount, itemsCreated,
                failedCreations, itemsUpdated,
                failedUpdates, itemsRemain, failedRemains,
                itemsDeleted, failedDeletions, itemsNoAction, trialRun, pathFinder.getCapabilityListUri());

    }

    private CORESitemapCollector getCORESitemapCollector() {
        return (CORESitemapCollector) getSitemapCollector();
    }

    private COREResourceManager getCOREResourceManager() {
        return (COREResourceManager) resourceManager;
    }



}