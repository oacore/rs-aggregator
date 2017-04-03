package nl.knaw.dans.rs.aggregator.sync;

import nl.knaw.dans.rs.aggregator.discover.*;
import nl.knaw.dans.rs.aggregator.xml.Capability;
import nl.knaw.dans.rs.aggregator.xml.ResourceSyncContext;
import org.apache.http.impl.client.CloseableHttpClient;

import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Giorgio Basile
 * @since 30/03/2017
 */
public class BaselineSynchronization implements Synchronization{

    private final ResourceSyncContext rsContext;
    private final CloseableHttpClient httpClient;
    private final URI uri;
    private Downloader downloader;

    public BaselineSynchronization(CloseableHttpClient httpClient, ResourceSyncContext rsContext, URI uri, Downloader downloader) {
        this.httpClient = httpClient;
        this.rsContext = rsContext;
        this.uri = uri;
        this.downloader = downloader;
    }

    public List<String> getResourceListLocations(){
        ResultIndex index = new ResultIndex();
        ResourceListExplorer resourceListExplorer = new ResourceListExplorer(httpClient, rsContext);
        resourceListExplorer.explore(uri, index);
        ResultIndexPivot indexPivot = new ResultIndexPivot(index);
        return indexPivot.listSitemapLocations(Capability.RESOURCELIST);
    }

    public List<String> getResourceUrls(String resourcelistPage){
        ResultIndex index = new ResultIndex();
        URI uri = URI.create(resourcelistPage);
        ResourcesExplorer rsExplorer = new ResourcesExplorer(httpClient, rsContext);
        rsExplorer.explore(uri, index);
        ResultIndexPivot indexPivot = new ResultIndexPivot(index);
        return indexPivot.listUrlLocations(Capability.RESOURCELIST);
    }

    @Override
    public void startSynchronization() {
        List<String> resourceListLocations = this.getResourceListLocations();
        for(String pageUrl : resourceListLocations){
            // System.out.println("**** FETCHING PAGE: " + pageUrl + " ****");
            List<String> resourceUrls = this.getResourceUrls(pageUrl);
            // System.out.println("# of resources: " + resourceUrls.size());
            List<URI> resources = resourceUrls.stream().map(URI::create).collect(Collectors.toList());
            startDownloader(resources);
        }

    }

    private void startDownloader(List<URI> resources){
        downloader.download(resources);
    }
}
