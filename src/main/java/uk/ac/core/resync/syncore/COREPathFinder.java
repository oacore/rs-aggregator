package uk.ac.core.resync.syncore;

import nl.knaw.dans.rs.aggregator.syncore.PathFinder;
import nl.knaw.dans.rs.aggregator.util.HashUtil;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.Optional;

/**
 * Created by mc26486 on 17/01/2018.
 */
public class COREPathFinder extends PathFinder {

    private static Logger logger = LoggerFactory.getLogger(COREPathFinder.class);

    public COREPathFinder(@Nonnull String baseDirectory, @Nonnull URI capabilityListUri) {
        super(baseDirectory, capabilityListUri);
    }

    @Override
    public File findResourceFilePath(@Nonnull URI uri) {
        return new File(getResourceDirectory(), extractCOREPath(uri).orElseThrow(() -> new IllegalArgumentException("The URI doesn't contain a matching URI")));
    }

    public Path getManifestFilePath(){
        return Paths.get(this.getResourceDirectory().getPath(), "manifest.xml");
    }

    private Optional<String> extractCOREPath(@Nonnull URI uri) {


        String coreId=StringUtils.substringAfterLast(uri.getPath(), "/");

        try {
            String idHash = HashUtil.computeHash("md5", IOUtils.toInputStream(coreId, "UTF-8"));
            String path = idHash.substring(0,3) + "/" + idHash.substring(3, 5) + "/" + coreId + ".json";
            return Optional.ofNullable(path);
        } catch (NoSuchAlgorithmException | IOException e) {
            e.printStackTrace();
        }
        return Optional.empty();
    }
}
