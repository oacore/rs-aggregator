package uk.ac.core.resync.sync;

import nl.knaw.dans.rs.aggregator.http.Result;
import nl.knaw.dans.rs.aggregator.sync.SyncWorker;
import nl.knaw.dans.rs.aggregator.syncore.PathFinder;
import nl.knaw.dans.rs.aggregator.util.RsProperties;
import nl.knaw.dans.rs.aggregator.xml.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.core.resync.syncore.COREResourceManager;

import javax.xml.bind.JAXBException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Created by mc26486 on 15/01/2018.
 */
public class CORESyncWorker extends SyncWorker implements SitemapDownloadedListener {

    private final static Logger logger = LoggerFactory.getLogger(CORESyncWorker.class);
    private static final int COREAPI_BATCH_SIZE = 100;
    private CORESitemapCollector collector;
    private PathFinder pathFinder;

    public CORESyncWorker() {
        super();
    }

    @Override
    protected void syncLocalResources(PathFinder pathFinder, RsProperties syncProps) {
        this.pathFinder=pathFinder;
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
                if (content instanceof Urlset){
                    Urlset urlSet = (Urlset) content;

                    this.batches(urlSet.getItemList(), COREAPI_BATCH_SIZE).forEach(e -> syncBatchItems(e));
                }

            }
        }
        collector.flushRecentItems();

    }


    private void syncBatchItems(List<UrlItem> urisToSync) {
        itemCount += urisToSync.size();


        urisToSync.forEach(e -> verifyAndAddToBatch(e));
        this.getCOREResourceManager().performBatchDownload();
        downloadCount++;
        Long numberOfVerified = this.doVerifyBatch(urisToSync);
        System.out.println(numberOfVerified);
    }

    private CORESitemapCollector getCORESitemapCollector() {
        return (CORESitemapCollector) getSitemapCollector();
    }

    private COREResourceManager getCOREResourceManager() {
        return (COREResourceManager) resourceManager;
    }

    private void verifyAndAddToBatch(UrlItem entry) {
        UrlItem item = entry;
        URI normalizedURI = entry.getNormalizedUri().get();
        String change = item.getMetadata().flatMap(RsMd::getChange).orElse(CH_REMAIN);
        boolean resourceExists = resourceManager.exists(normalizedURI);

        logger.debug("------> {} {}, exists={}, normalizedURI={}", itemCount, change, resourceExists, normalizedURI);

        if (CH_REMAIN.equalsIgnoreCase(change)) {
            itemsRemain++;
            if (resourceExists) {
                this.getResourceManager().keep(normalizedURI);
            } else {
                if (actionAllowed(normalizedURI)) {
                    this.getResourceManager().create(normalizedURI);
                } else {
                    failedRemains++;
                }
            }
        } else if (CH_CREATED.equalsIgnoreCase(change)) {
            itemsCreated++;
            if (actionAllowed(normalizedURI)) {
                this.getResourceManager().create(normalizedURI);
            } else {
                failedCreations++;
            }
        } else if (CH_UPDATED.equalsIgnoreCase(change)) {
            itemsUpdated++;
            if (actionAllowed(normalizedURI)) {
                this.getResourceManager().update(normalizedURI);
            } else {
                failedUpdates++;
            }
        } else if (CH_DELETED.equalsIgnoreCase(change) && resourceExists) {
            if (actionAllowed(normalizedURI) && this.getResourceManager().delete(normalizedURI)) {
                itemsDeleted++;
            } else {
                failedDeletions++;
            }
        } else if (CH_DELETED.equalsIgnoreCase(change) && !resourceExists) {
            itemsNoAction++;
        }
    }

    public static <T> Stream<List<T>> batches(List<T> source, int length) {
        if (length <= 0)
            throw new IllegalArgumentException("length = " + length);
        int size = source.size();
        if (size <= 0)
            return Stream.empty();
        int fullChunks = (size - 1) / length;
        return IntStream.range(0, fullChunks + 1).mapToObj(
                n -> source.subList(n * length, n == fullChunks ? size : (n + 1) * length));
    }

    protected Long doVerifyBatch(List<UrlItem> urisToSync) {
        try {
            String xmlString = new String(Files.readAllBytes(this.getCOREResourceManager().getManifestFilePath()));

            InputStream inStream = new ByteArrayInputStream(xmlString.getBytes("UTF-8"));
            RsRoot rsRoot = new RsBuilder(this.getSitemapCollector().getRsContext()).setInputStream(inStream).build().orElse(null);
            List<UrlItem> itemList = rsRoot.getItemList();

            Long numberOfValidItems = itemList.stream().map(item -> super.doVerify(this.matchURIToItemToDownload(urisToSync, item), item)).collect(Collectors.counting());

            return numberOfValidItems;
        } catch (IOException | JAXBException e) {
            logger.error("Error reading manifest.xml  ", e);
        }
        return 0L;
    }

    private URI matchURIToItemToDownload(List<UrlItem> urisToSync, UrlItem item) {
        return urisToSync.stream().filter(e -> e.getNormalizedUri().get().toASCIIString().equals(item.getLoc())).findFirst().get().getNormalizedUri().get();
    }


}