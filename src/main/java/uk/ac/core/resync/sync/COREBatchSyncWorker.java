package uk.ac.core.resync.sync;

import nl.knaw.dans.rs.aggregator.http.Result;
import nl.knaw.dans.rs.aggregator.sync.SyncWorker;
import nl.knaw.dans.rs.aggregator.syncore.PathFinder;
import nl.knaw.dans.rs.aggregator.util.RsProperties;
import nl.knaw.dans.rs.aggregator.xml.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.core.resync.syncore.COREBatchResourceManager;

import javax.xml.bind.JAXBException;
import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Created by mc26486 on 15/01/2018.
 */
public class COREBatchSyncWorker extends SyncWorker implements SitemapDownloadedListener {

    private final static Logger logger = LoggerFactory.getLogger(COREBatchSyncWorker.class);
    public static final int DEFAULT_BATCH_SIZE = 100;
    private int batchSize;
    private CORESitemapCollector collector;
    private PathFinder pathFinder;
    private FileWriter fileWriter;
    private RsRoot mainManifest;
    private Integer attempts;
    private final static Integer MAX_RETRIES = 5;
    private static final int[] FIBONACCI = new int[]{1, 2, 3, 5, 8, 13};
    private int maxRecordsToDownload;
    private boolean forceStop;

    public COREBatchSyncWorker() {
        super();

    }

    @Override
    protected void syncLocalResources(PathFinder pathFinder, RsProperties syncProps) {
        this.logger.info("Batch size is: {}", batchSize);



        this.logger.info(this.getCOREBatchResourceManager().isManualUpdate()?"THIS PROGRAM WON'T UPDATE THE CHANGELIST ENDPOINT":"THIS PROGRAM WILL AUTOMATICALLY UPDATE THE CHANGELIST ENDPOINT");

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
        logger.info(String.valueOf(collector.getUltimateChangeListFrom().toInstant().toEpochMilli()));
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
        if (!this.getCOREBatchResourceManager().isManualUpdate()) {
            Path path = Paths.get("cfg/uri-list.txt");
            try (BufferedWriter writer = Files.newBufferedWriter(path)) {
                writer.write("https://core.ac.uk/resync/changelist/" + collector.getUltimateChangeListFrom().toInstant().toEpochMilli() + "/changelist_index.xml");
            } catch (IOException e) {
                logger.error("Failed to write cfg/uri-list.txt");
            }
        }
        logger.info("Next changelist to download https://core.ac.uk/resync/changelist/{}/changelist_index.xml", System.currentTimeMillis());
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

                    this.batches(urlSet.getItemList(), this.getBatchSize()).forEach(e -> syncBatchItems(e));
                }

            }
        }
        collector.flushRecentItems();

    }


    private void syncBatchItems(List<UrlItem> urisToSync) {
        if (forceStop){
            return;
        }
        itemCount += urisToSync.size();
        this.getCOREBatchResourceManager().clearBatch();
        urisToSync.forEach(e -> verifyAndAddToBatch(e));
        this.attempts = 1;
        boolean batchDownloadSuccess = this.getCOREBatchResourceManager().performBatchDownload();
        downloadCount++;
        if (!batchDownloadSuccess) {
            totalFailures++;
        }

        while (!batchDownloadSuccess && this.attempts < MAX_RETRIES) {
            this.attempts++;
            Integer toWait = FIBONACCI[this.attempts] * 1;
            logger.info("Exponential backoff. Waiting for {}", toWait);
            try {
                Thread.sleep(toWait);
            } catch (InterruptedException e) {
                logger.error("Exponential backoff interrupted");
            }

            batchDownloadSuccess = this.getCOREBatchResourceManager().performBatchDownload();
            downloadCount++;
            if (!batchDownloadSuccess) {
                totalFailures++;
            }

        }

        Long numberOfVerified = this.doVerifyBatch(urisToSync);
        logger.info("====> synchronized={}, new ResourceList={}, items={}, verified={}, " +
                        "failures={}, downloads={} [success/failures] " +
                        "created={}/{}, updated={}/{}, remain={}/{}, deleted={}/{}, " +
                        "no_action={}, trial run={}, resource set={}",
                syncComplete, collector.hasNewResourceList(), itemCount, verifiedItems, totalFailures, downloadCount, itemsCreated,
                failedCreations, itemsUpdated,
                failedUpdates, itemsRemain, failedRemains,
                itemsDeleted, failedDeletions, itemsNoAction, trialRun, pathFinder.getCapabilityListUri());
        logger.info("Max records: {}", this.maxRecordsToDownload);
        if (itemCount>=this.maxRecordsToDownload){
            logger.info("Completing download because reached the maxRecordsToDownload parameter");
            this.forceStop=true;
        }
    }

    private CORESitemapCollector getCORESitemapCollector() {
        return (CORESitemapCollector) getSitemapCollector();
    }

    private COREBatchResourceManager getCOREBatchResourceManager() {
        return (COREBatchResourceManager) resourceManager;
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
            String xmlString = new String(Files.readAllBytes(this.getCOREBatchResourceManager().getManifestFilePath()));

            InputStream inStream = new ByteArrayInputStream(xmlString.getBytes("UTF-8"));
            RsRoot rsRoot = new RsBuilder(this.getSitemapCollector().getRsContext()).setInputStream(inStream).build().orElse(null);
            List<UrlItem> itemList = rsRoot.getItemList();
//            if (itemList.size() > 0) {
//                if (mainManifest==null){
//                    mainManifest=rsRoot;
//                }
//                this.mainManifest.getItemList().addAll(itemList);
//                Path toWrite = Paths.get(this.getCOREBatchResourceManager().getCOREPathfinder().getResourceDirectory().getPath(), "mainManifest.xml" );
//                Files.write(toWrite, (new RsBuilder(this.getSitemapCollector().getRsContext()).toXml(this.mainManifest, true)).getBytes());
//
//
//            }
            Long numberOfValidItems = itemList.stream().map(item -> {
                try {
                    return super.doVerify(this.matchURIToItemToDownload(urisToSync, item), item);
                } catch (Exception e) {
                    return false;
                }
            }).collect(Collectors.counting());

            return numberOfValidItems;
        } catch (IOException | JAXBException e) {
            logger.error("Error reading manifest.xml  ", e);
        }
        return 0L;
    }

    private URI matchURIToItemToDownload(List<UrlItem> urisToSync, UrlItem item) {
        return urisToSync.stream().filter(e -> e.getLoc().equals(item.getLoc())).findFirst().orElse(null).getNormalizedUri().get();
    }

    public String printMetrics(){

        String metrics = Arrays.stream(new int[] {itemCount, verifiedItems, totalFailures, downloadCount, itemsCreated,
                failedCreations, itemsUpdated,
                failedUpdates, itemsRemain, failedRemains,itemsDeleted, failedDeletions, itemsNoAction})
                .mapToObj(String::valueOf)
                .collect(Collectors.joining(","));
        return metrics;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batch_size) {
        this.batchSize = batch_size;
    }

    public int getMaxRecordsToDownload() {
        return maxRecordsToDownload;
    }

    public void setMaxRecordsToDownload(int maxRecordsToDownload) {
        this.maxRecordsToDownload = maxRecordsToDownload;
    }
}