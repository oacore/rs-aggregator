package nl.knaw.dans.rs.aggregator.sync;

import nl.knaw.dans.rs.aggregator.xml.ResourceSyncContext;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import javax.xml.bind.JAXBException;
import java.io.File;
import java.net.URI;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;

/**
 * @author Giorgio Basile
 * @since 30/03/2017
 */
public class BaselineSynchronizationTest {

    /*private CloseableHttpClient createHttpClient_AcceptsUntrustedCerts() throws KeyStoreException, NoSuchAlgorithmException, KeyManagementException {
        HttpClientBuilder b = HttpClientBuilder.create();
        SSLContext sslContext = new SSLContextBuilder().loadTrustMaterial(null, (TrustStrategy) (x509Certificates, s) -> true).build();
        b.setSSLContext(sslContext);
        return b.build();
    }*/

    private final static String TMP_FOLDER = "tmp/elsevier-meta";

    @Test
    public void testSync() {
        try {

            CloseableHttpClient httpClient = HttpClients.createDefault();
            BaselineSynchronization baselineSynchronization = new BaselineSynchronization(httpClient,
                    new ResourceSyncContext(),
                    URI.create("http://publisher-connector.core.ac.uk/resourcesync/sitemaps/elsevier/metadata/capabilitylist.xml"),
                    new FileSystemDownloader(httpClient, new File(TMP_FOLDER), URI.create("http://publisher-connector.core.ac.uk/resourcesync")));
            baselineSynchronization.startSynchronization();

        } catch (JAXBException e) {
            e.printStackTrace();
        }

    }


}