package nl.knaw.dans.rs.aggregator.examples;

import nl.knaw.dans.rs.aggregator.sync.BaselineSynchronization;
import nl.knaw.dans.rs.aggregator.sync.FileSystemDownloader;
import nl.knaw.dans.rs.aggregator.xml.ResourceSyncContext;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.net.URI;

/**
 * @author Giorgio Basile
 * @since 30/03/2017
 */
public class BaselineSynchronizationExample {


    private final static String TMP_FOLDER = "tmp/elsevier-meta";
    private final static String ELSEVIER_META_URL = "http://publisher-connector.core.ac.uk/resourcesync/sitemaps/elsevier/metadata/capabilitylist.xml";
    private final static String ELSEVIER_PDF_URL = "http://publisher-connector.core.ac.uk/resourcesync/sitemaps/elsevier/pdf/capabilitylist.xml";
    private final static String ROOT_URL = "http://publisher-connector.core.ac.uk/resourcesync";

    public static void main(String args[]) {
        try {

            CloseableHttpClient httpClient = HttpClients.createDefault();
            BaselineSynchronization baselineSynchronization = new BaselineSynchronization(httpClient,
                    new ResourceSyncContext(),
                    URI.create(ELSEVIER_META_URL),
                    new FileSystemDownloader(httpClient, new File(TMP_FOLDER), URI.create(ROOT_URL)));
            baselineSynchronization.startSynchronization();

        } catch (JAXBException e) {
            e.printStackTrace();
        }

    }


}