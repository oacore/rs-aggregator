package uk.ac.core.resync.syncore;

import com.google.gson.Gson;
import nl.knaw.dans.rs.aggregator.http.Result;
import nl.knaw.dans.rs.aggregator.sync.FsResourceManager;
import nl.knaw.dans.rs.aggregator.util.HashUtil;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.core.resync.http.COREResourceReader;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Created by mc26486 on 16/01/2018.
 */
public class COREBatchResourceManager extends FsResourceManager implements BatchResourceManager {

    private static Logger logger = LoggerFactory.getLogger(COREBatchResourceManager.class);

    private String coreApiEndpoint;

    Set<URI> buffer = new HashSet<>();
    private final static String CORE_DEFAULT_API_ENDPOINT = "http://core.ac.uk/datadump";

    @Override
    public boolean create(@Nonnull URI normalizedURI) {
        return prepareToDownload(normalizedURI);
    }

    @Override
    public boolean update(@Nonnull URI normalizedURI) {
        return prepareToDownload(normalizedURI);
    }

    private boolean prepareToDownload(URI normalizedURI) {

        return buffer.add(normalizedURI);
    }

    public boolean addToBatch(@Nonnull URI normalizedURI) {
        return prepareToDownload(normalizedURI);
    }

    @Override
    public boolean performBatchDownload() {
        if (buffer.isEmpty()){
            return true;
        }
        String requestParameters = new Gson().toJson(buffer);
        boolean downloaded = false;
        File dumpPath = null;
        Result<File> result = null;
        try {
            String hashname = HashUtil.computeHash("md5", IOUtils.toInputStream(requestParameters, "UTF-8"));
            dumpPath = File.createTempFile(hashname, ".zip");
            result = getCOREResourceReader().readWithPost(this.getCoreApiEndpoint(), requestParameters, dumpPath);
            if (result.getStatusCode()<300) {
                unzipDump(dumpPath);
            }
        } catch (NoSuchAlgorithmException | IOException | URISyntaxException e) {
            logger.error("Error in creating path for download", e);

        }
        if (result.getContent().isPresent()) {
            downloaded = true;
            logger.debug("Downloaded {} --> {}", this.getCoreApiEndpoint(), dumpPath);
        } else {
            logger.warn("Failed download of {}: ", this.getCoreApiEndpoint(), result.lastError());
        }
        buffer.clear();
        return downloaded;

    }

    private void unzipDump(File resourcePath) throws IOException {
        byte[] buffer = new byte[1024];
        ZipInputStream zis = new ZipInputStream(new FileInputStream(resourcePath));
        ZipEntry zipEntry = zis.getNextEntry();
        while (zipEntry != null) {
            String filename = zipEntry.getName();
            if (!filename.equals("manifest.xml")) {
                String foldername = StringUtils.substringBeforeLast(filename, "/");
                File directory = new File(getPathFinder().getResourceDirectory(), foldername);
                directory.mkdirs();
            }
            File resourceFile = new File(getPathFinder().getResourceDirectory(), filename);
            FileOutputStream fos = new FileOutputStream(resourceFile);
            int len;
            while ((len = zis.read(buffer)) > 0) {
                fos.write(buffer, 0, len);
            }
            fos.close();
            zipEntry = zis.getNextEntry();
        }
        zis.closeEntry();
        zis.close();
    }

    private COREResourceReader getCOREResourceReader() {
        return (COREResourceReader) getResourceReader();
    }

    public Path getManifestFilePath() {
        return this.getCOREPathfinder().getManifestFilePath();
    }

    public COREPathFinder getCOREPathfinder() {
        return (COREPathFinder) this.getPathFinder();
    }

    public String getCoreApiEndpoint() {
        if (coreApiEndpoint == null){
            coreApiEndpoint = CORE_DEFAULT_API_ENDPOINT;
        }
        return coreApiEndpoint;
    }

    public void setCoreApiEndpoint(String coreApiEndpoint) {
        this.coreApiEndpoint = coreApiEndpoint;
    }
}
