package uk.ac.core.resync.syncore;

import nl.knaw.dans.rs.aggregator.sync.FsResourceManager;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by mc26486 on 16/01/2018.
 */
public class COREResourceManager extends FsResourceManager  {

    private static Logger logger = LoggerFactory.getLogger(COREResourceManager.class);

    private String coreApiEndpoint;

    private final static String CORE_DEFAULT_API_ENDPOINT = "https://fileserver.core.ac.uk/datadump";

    @Override
    public boolean create(@Nonnull URI normalizedURI) {
        URIBuilder builder = new URIBuilder(normalizedURI);
        builder.setParameter("parts", "all").setParameter("action", "finish");
        try {
            return super.create(builder.build());
        } catch (URISyntaxException e) {
            logger.error("Error creating uri to download", e);
        }
        return false;
    }

    @Override
    public boolean update(@Nonnull URI normalizedURI) {
        URIBuilder builder = new URIBuilder(normalizedURI);
        builder.setParameter("apiKey", "1cYpQHHJBM61XstNV9NxrD2CvGOe10oK");
        try {
            return super.update(builder.build());
        } catch (URISyntaxException e) {
            logger.error("Error creating uri to download", e);
        }
        return false;
    }
}
