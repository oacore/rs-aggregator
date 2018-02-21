package uk.ac.core.resync.http;

import nl.knaw.dans.rs.aggregator.http.RemoteException;
import nl.knaw.dans.rs.aggregator.http.ResourceReader;
import nl.knaw.dans.rs.aggregator.http.Result;
import nl.knaw.dans.rs.aggregator.util.LambdaUtil;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by mc26486 on 17/01/2018.
 */
public class COREResourceReader extends ResourceReader {

    private static Logger logger = LoggerFactory.getLogger(COREResourceReader.class);

    private URI currentUri;
    private COREResourceReader resourceReader;

    public COREResourceReader(CloseableHttpClient httpClient) {
        super(httpClient);
    }

    public Result<File> readWithPost(String url, String parameters, File file) throws URISyntaxException {
        URI uri = new URI(url);
        return readWithPost(uri,parameters, file);
    }

    public Result<File> readWithPost(URI uri, String parameters, File file) {
        currentFile = file;
        return executePost(uri, parameters,fileWriter);
    }

    public Result<File> read(URI uri){
        return this.execute(uri, fileWriter);
    }

    public <R> Result<R> executePost(URI uri, String parameters, LambdaUtil.BiFunction_WithExceptions<URI, HttpResponse, R, ?> func) {
        logger.debug("Executing GET on uri {}", uri);
        currentUri = uri;
        Result<R> result = new Result<R>(uri);
        HttpPost request = new HttpPost(uri);

        CloseableHttpResponse response = null;
        try  {
            System.out.println("parameters = " + parameters);
            HttpEntity entity = new ByteArrayEntity(parameters.getBytes("UTF-8"));
            request.setEntity(entity);
            response = getHttpClient().execute(request);
            int statusCode = response.getStatusLine().getStatusCode();
            result.setStatusLine(response.getStatusLine().toString());
            logger.info("Received {} from {}", response.getStatusLine(), uri);
            result.setStatusCode(statusCode);
            if (isKeepingHeaders()) {
                for (Header header : response.getAllHeaders()) {
                    result.getHeaders().put(header.getName(), header.getValue());
                }
            }
            if (statusCode < 200 || statusCode > 299) {
                result.addError(new RemoteException(statusCode, response.getStatusLine().getReasonPhrase(), uri));
            } else {
                result.accept(func.apply(uri, response));
            }
        } catch (Exception e) {
            logger.error("Error executing GET on uri {}", uri, e);
            result.addError(e);
        } finally {
            closeResponse(response);
        }
        return result;
    }


}
