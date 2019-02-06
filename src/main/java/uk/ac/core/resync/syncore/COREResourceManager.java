package uk.ac.core.resync.syncore;

import com.google.gson.Gson;
import nl.knaw.dans.rs.aggregator.http.Result;
import nl.knaw.dans.rs.aggregator.sync.FsResourceManager;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.core.resync.http.COREResourceReader;

import javax.annotation.Nonnull;
import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by mc26486 on 16/01/2018.
 */
public class COREResourceManager extends FsResourceManager {

    private static Logger logger = LoggerFactory.getLogger(COREResourceManager.class);

    private String coreApiEndpoint;
    private int maxRecordsToDownload;

    private final static String CORE_DEFAULT_API_ENDPOINT =  "https://core.ac.uk/datadump";


    @Override
    public boolean create(@Nonnull URI normalizedURI) {

        return download(normalizedURI);
    }



    @Override
    public boolean update(@Nonnull URI normalizedURI) {
        return download(normalizedURI);
    }

    private boolean download(URI uri) {

        String requestParameters = new Gson().toJson(new URI[]{uri});
        logger.info("requestParamters" + requestParameters);
        String filename = FilenameUtils.getBaseName(uri.getPath()) + ".json";
        File file = new File(getPathFinder().getResourceDirectory(), filename);
        Result<File> result = null;
        boolean downloaded=false;
        try {
            result = getCOREResourceReader().readWithPost(this.getCoreApiEndpoint(), requestParameters, file);
        } catch (URISyntaxException e) {
            logger.error("Error download ", e);
        }
        if (result.getContent().isPresent()) {
            downloaded = true;
            logger.debug("Downloaded {} --> {}", this.getCoreApiEndpoint(), file);
        } else {
            logger.warn("Failed download of {}: ", this.getCoreApiEndpoint(), result.lastError());
        }
        return downloaded;
    }

    private COREResourceReader getCOREResourceReader() {
        return (COREResourceReader) getResourceReader();
    }

    public String getCoreApiEndpoint() {
        if (coreApiEndpoint == null) {
            coreApiEndpoint = CORE_DEFAULT_API_ENDPOINT;
        }
        return coreApiEndpoint;
    }

    public int getMaxRecordsToDownload() {
        return maxRecordsToDownload;
    }

    public void setMaxRecordsToDownload(int maxRecordsToDownload) {
        this.maxRecordsToDownload = maxRecordsToDownload;
    }
}
