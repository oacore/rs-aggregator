package uk.ac.core.resync.sync;

import nl.knaw.dans.rs.aggregator.discover.RsExplorer;
import nl.knaw.dans.rs.aggregator.http.Result;
import nl.knaw.dans.rs.aggregator.sync.SitemapCollector;
import nl.knaw.dans.rs.aggregator.syncore.PathFinder;
import nl.knaw.dans.rs.aggregator.util.RsProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.core.resync.discover.CORERsExplorer;

/**
 * Created by mc26486 on 08/02/2018.
 */
public class CORESitemapCollector extends SitemapCollector {

    private static Logger logger = LoggerFactory.getLogger(CORESitemapCollector.class);

    public void collectSitemaps(PathFinder pathFinder, RsProperties syncProps, SitemapDownloadedListener sitemapDownloadedListener) {
        reset();
        RsExplorer explorer = new CORERsExplorer(getHttpClient(), getRsContext(), sitemapDownloadedListener)
                .withConverter(getConverter())
                .withFollowChildLinks(true)
                .withFollowIndexLinks(false)
                .withFollowParentLinks(false);
        logger.info("exploring " + pathFinder.getCapabilityListUri());
        currentIndex = explorer.explore(pathFinder.getCapabilityListUri());

        invalidUris = currentIndex.getInvalidUris();
        for (String invalidUri : invalidUris) {
            logger.warn("Found invalid URI: {}", invalidUri);
        }

        for (Result<?> result : currentIndex.getResultMap().values()) {
            if (result.hasErrors()) {
                errorResults.add(result);
                for (Throwable error : result.getErrors()) {
                    logger.warn("Result has errors. URI: {}, msg: {}", pathFinder.getCapabilityListUri(), error.getMessage());
                }
            } else {
                analyze(result);
                logger.info("analyzed " + pathFinder.getCapabilityListUri());

            }
        }
        reportResults(pathFinder, syncProps);

        setNewResourceListFound(pathFinder);
    }

    public void flushRecentItems() {
        this.recentItems.clear();
    }


}
