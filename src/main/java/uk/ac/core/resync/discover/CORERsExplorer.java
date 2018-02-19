package uk.ac.core.resync.discover;

import nl.knaw.dans.rs.aggregator.discover.ResultIndex;
import nl.knaw.dans.rs.aggregator.discover.RsExplorer;
import nl.knaw.dans.rs.aggregator.http.Result;
import nl.knaw.dans.rs.aggregator.xml.*;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.core.resync.sync.SitemapDownloadedListener;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;

/**
 * Created by mc26486 on 09/02/2018.
 */
public class CORERsExplorer extends RsExplorer {

    private static Logger logger = LoggerFactory.getLogger(CORERsExplorer.class);

    private final SitemapDownloadedListener sitemapDownloadedListener;

    public CORERsExplorer(CloseableHttpClient httpClient, ResourceSyncContext rsContext, SitemapDownloadedListener sitemapDownloadedListener) {
        super(httpClient, rsContext);
        this.sitemapDownloadedListener=sitemapDownloadedListener;
    }

    @SuppressWarnings ("unchecked")
    @Override
    public Result<RsRoot> explore(URI uri, ResultIndex index) {
        Result<RsRoot> result = execute(uri, getConverter());
        logger.info("Executing " + uri.toASCIIString());
        index.add(result);
        if (result.hasErrors()) {
            return result;
        }
        Capability capability = extractCapability(result);

        if (followParentLinks) {
            // rs:ln rel="up" -> points to parent document, a urlset.
            String parentLink = result.getContent().map(rsRoot -> rsRoot.getLink("up")).orElse(null);
            if (parentLink != null && !index.contains(parentLink)) {
                try {
                    URI parentUri = new URI(parentLink);
                    Result<RsRoot> parentResult = explore(parentUri, index);
                    result.addParent(parentResult);
                    verifyUpRelation(result, parentResult, capability);
                } catch (URISyntaxException e) {
                    index.addInvalidUri(parentLink);
                    result.addError(e);
                    result.addInvalidUri(parentLink);
                }
            }
        }

        if (followIndexLinks) {
            // rs:ln rel="index" -> points to parent index, a sitemapindex.
            String indexLink = result.getContent().map(rsRoot -> rsRoot.getLink("index")).orElse(null);
            if (indexLink != null && !index.contains(indexLink)) {
                try {
                    URI indexUri = new URI(indexLink);
                    Result<RsRoot> indexResult = explore(indexUri, index);
                    result.addParent(indexResult);
                    verifyIndexRelation(result, indexResult, capability);
                } catch (URISyntaxException e) {
                    index.addInvalidUri(indexLink);
                    result.addError(e);
                    result.addInvalidUri(indexLink);
                }
            }
        }

        if (followChildLinks) {
            // elements <url> or <sitemap> have the location of the children of result.
            // children of Urlset with capability resourcelist, resourcedump, changelist, changedump
            // are the resources them selves. do not explore these with this explorer.
            String xmlString = result.getContent()
                    .map(RsRoot::getMetadata).flatMap(RsMd::getCapability).orElse("invalid");

            boolean isSitemapindex = result.getContent().map(rsRoot -> rsRoot instanceof Sitemapindex).orElse(false);

            if (Capability.levelfor(xmlString) > Capability.RESOURCELIST.level || isSitemapindex) {
                List<RsItem> itemList = result.getContent().map(RsRoot::getItemList).orElse(Collections.emptyList());
                for (RsItem item : itemList) {
                    String childLink = item.getLoc();
                    if (childLink != null && !index.contains(childLink)) {
                        try {
                            URI childUri = new URI(childLink);
                            logger.info("Exploring " + childLink);

                            Result<RsRoot> childResult = explore(childUri, index);
                            result.addChild(childResult);
                            verifyChildRelation(result, childResult, capability);
                            sitemapDownloadedListener.sitemapDownloaded(childResult);
                        } catch (URISyntaxException e) {
                            index.addInvalidUri(childLink);
                            result.addError(e);
                            result.addInvalidUri(childLink);
                        }
                    }
                }
            }
        }

        return result;
    }
}
