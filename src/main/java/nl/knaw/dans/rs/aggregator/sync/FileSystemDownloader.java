package nl.knaw.dans.rs.aggregator.sync;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import java.util.List;

/**
 * @author Giorgio Basile
 * @since 30/03/2017
 */
public class FileSystemDownloader extends Downloader {

    private File targetFolder;
    private URI rootUri;

    public FileSystemDownloader(CloseableHttpClient httpClient, File targetFolder, URI rootUri) {
        super(httpClient);
        this.targetFolder = targetFolder;
        this.rootUri = rootUri;
        if(!targetFolder.exists()){
            targetFolder.mkdirs();
        }
    }

    @Override
    public void download(List<URI> resources) {
        resources.parallelStream().forEach(this::downloadTask);
    }

    public Downloader newInstance(CloseableHttpClient httpClient, File targetFolder) {
        return new FileSystemDownloader(httpClient, targetFolder, rootUri);
    }

    private void downloadTask(URI uri){
        try (CloseableHttpResponse response = getHttpClient().execute(new HttpGet(uri))) {
            HttpEntity entity = response.getEntity();
            File resourceFile = new File(targetFolder, rootUri.relativize(uri).toString());
            if(!resourceFile.exists()) {
                resourceFile.getParentFile().mkdirs();
                resourceFile.createNewFile();
            }
            if (entity != null) {
                try (FileOutputStream outstream = new FileOutputStream(resourceFile)) {
                    entity.writeTo(outstream);
                }
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
}
