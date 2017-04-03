package nl.knaw.dans.rs.aggregator.sync;

import org.apache.http.impl.client.CloseableHttpClient;

import java.net.URI;
import java.util.List;

/**
 * @author Giorgio Basile
 * @since 30/03/2017
 */
public abstract class Downloader {

    public static final String FILE_SYSTEM = "file_system";

    private final CloseableHttpClient httpClient;

    public Downloader(CloseableHttpClient httpClient){
        this.httpClient = httpClient;
    }

    public abstract void download(List<URI> resources);

    public CloseableHttpClient getHttpClient() {
        return httpClient;
    }
}
