package uk.ac.core.resync.sync;

import nl.knaw.dans.rs.aggregator.http.Result;
import nl.knaw.dans.rs.aggregator.xml.RsRoot;

/**
 * Created by mc26486 on 08/02/2018.
 */
public interface SitemapDownloadedListener {

    public void sitemapDownloaded(Result<RsRoot> result);
}
