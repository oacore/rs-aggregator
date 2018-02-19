package nl.knaw.dans.rs.aggregator.discover;

import nl.knaw.dans.rs.aggregator.util.LambdaUtil;
import nl.knaw.dans.rs.aggregator.xml.*;
import org.apache.http.HttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import nl.knaw.dans.rs.aggregator.xml.Capability;
import nl.knaw.dans.rs.aggregator.xml.ResourceSyncContext;
import nl.knaw.dans.rs.aggregator.xml.RsBuilder;
import nl.knaw.dans.rs.aggregator.xml.RsItem;
import nl.knaw.dans.rs.aggregator.xml.RsMd;
import nl.knaw.dans.rs.aggregator.xml.RsRoot;
import nl.knaw.dans.rs.aggregator.xml.Sitemapindex;

/**
 * @author Giorgio Basile
 * @since 30/03/2017
 */
public class ResourceListExplorer extends AbstractUriExplorer {

    private final ResourceSyncContext rsContext;

    public ResourceListExplorer(CloseableHttpClient httpClient, ResourceSyncContext rsContext) {
        super(httpClient);
        this.rsContext = rsContext;
    }

    @SuppressWarnings ("unchecked")
    @Override
    public Result<RsRoot> explore(URI uri, ResultIndex index) {
        Result<RsRoot> result = execute(uri, rsConverter);
        index.add(result);
        Capability capability = extractCapability(result);

        // elements <url> or <sitemap> have the location of the children of result.
        // children of Urlset with capability resourcelist, resourcedump, changelist, changedump
        // are the resources them selves. do not explore these with this explorer.

        String xmlString = result.getContent()
                .map(RsRoot::getMetadata).flatMap(RsMd::getCapability).orElse("invalid");

        boolean isSitemapindex = result.getContent().map(rsRoot -> rsRoot instanceof Sitemapindex).orElse(false);

        if ((Capability.levelfor(xmlString) > Capability.RESOURCELIST.level || isSitemapindex) &&
                !xmlString.equals(Capability.CHANGELIST.getXmlValue())) {

            List<RsItem> itemList = result.getContent().map(RsRoot::getItemList).orElse(Collections.emptyList());
            for (RsItem item : itemList) {
                String childLink = item.getLoc();
                if (childLink != null && !index.contains(childLink)) {
                    try {
                        URI childUri = new URI(childLink);
                        if(!isSitemapindex){
                            Result<RsRoot> childResult = explore(childUri, index);
                            result.addChild(childResult);
                            verifyChildRelation(result, childResult, capability);
                        }

                    } catch (URISyntaxException e) {
                        index.addInvalidUri(childLink);
                        result.addError(e);
                        result.addInvalidUri(childLink);
                    }
                }
            }
        }
        return result;
    }

    private Capability extractCapability(Result<RsRoot> result) {
        String xmlString = result.getContent()
                .map(RsRoot::getMetadata).flatMap(RsMd::getCapability).orElse("");
        Capability capa = null;
        try {
            capa = Capability.forString(xmlString);
        } catch (IllegalArgumentException e) {
            result.addError(new RemoteResourceSyncFrameworkException(
                    String.format("invalid value for capability: '%s'", xmlString)));
        }
        return capa;
    }

    private void verifyChildRelation(Result<RsRoot> result, Result<RsRoot> childResult, Capability capability) {
        if (result.getContent().isPresent() && childResult.getContent().isPresent()) {
            Capability childCapa = extractCapability(childResult);

            if (capability != null && !capability.verifyChildRelation(childCapa)) {
                result.addError(new RemoteResourceSyncFrameworkException(
                        String.format("invalid child relation: Expected %s, found '%s'",
                                Arrays.toString(capability.getChildRelationsXmlValues()),
                                childCapa == null ? "<no relation>" : childCapa.xmlValue)));
            }

            // child relation to document of same capability only allowed if document is sitemapIndex
            if (capability != null && capability == childCapa) {
                if (!result.getContent().map(content -> content instanceof Sitemapindex).orElse(true)) {
                    result.addError(new RemoteResourceSyncFrameworkException(
                            String.format("invalid child relation: relation to same capability '%s' " +
                                    "and document is not '<sitemapindex>'", capability.xmlValue)));
                }
            }
        }
    }

    private ResourceSyncContext getRsContext() {
        return rsContext;
    }

    private LambdaUtil.Function_WithExceptions<HttpResponse, RsRoot, Exception> rsConverter = (response) -> {
        InputStream inStream = response.getEntity().getContent();
        return new RsBuilder(this.getRsContext()).setInputStream(inStream).build().orElse(null);
    };

}
