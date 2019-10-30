package uk.ac.core.resync.syncore;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.util.IOUtils;
import org.apache.commons.lang3.StringUtils;
import uk.ac.core.configuration.S3Configuration;

import javax.annotation.Nonnull;
import java.io.*;
import java.net.URI;
import java.util.Date;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class COREBatchAWSResourceManager extends COREBatchResourceManager {

    private final S3Configuration s3Configuration;
    private AmazonS3 s3Client=null;
    private String bucketName="fastsync-test-bucket";

    public COREBatchAWSResourceManager(boolean manualUpdate, S3Configuration s3Configuration) {
        super(manualUpdate);
        this.s3Configuration = s3Configuration;
        AWSCredentials credentials = new BasicAWSCredentials(
                this.s3Configuration.getAccessKey(),
                this.s3Configuration.getSecretKey()
        );
        this.s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(this.s3Configuration.getRegion())
                .build();
        this.bucketName=this.s3Configuration.getBucketName();
    }

    @Override
    protected void unzipDump(File resourcePath) throws IOException {
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
            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setLastModified(Date.from(zipEntry.getLastModifiedTime().toInstant()));
            objectMetadata.setContentLength(zipEntry.getSize());
            PutObjectResult putObjectResult = this.s3Client.putObject(
                    this.bucketName,filename, convertZipInputStreamToInputStream(zis), objectMetadata
            );
            putObjectResult.getVersionId();
            zipEntry = zis.getNextEntry();
        }
        zis.closeEntry();
        zis.close();
    }

    private InputStream convertZipInputStreamToInputStream(
            final ZipInputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IOUtils.copy(in, out);
        InputStream is = new ByteArrayInputStream(out.toByteArray());
        return is;
    }

    @Override
    public boolean exists(@Nonnull URI normalizedURI) {
        String path = this.getPathFinder().findResourceFilePath(normalizedURI).getPath()
                .replace(this.getPathFinder().getResourceDirectory().getPath(), "");
        if (path.startsWith("/")){
            path = path.substring(1);
        }
        return this.s3Client.doesObjectExist(this.bucketName,path);
    }

    @Override
    public boolean delete(@Nonnull URI normalizedURI) {
         this.s3Client.deleteObject(this.bucketName, normalizedURI.getPath());
         return true;
    }
}
