package nl.knaw.dans.rs.aggregator.discover;

import nl.knaw.dans.rs.aggregator.xml.*;
import org.junit.Test;
import org.mockserver.matchers.Times;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;

import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

/**
 * @author Giorgio Basile
 * @since 31/03/2017
 */
public class ResourceListExplorerTest extends AbstractRemoteTest{

    private static final String CAPABILITYLIST_PATH = "/dataset1/capabilitylist.xml";
    private static final String RESOURCELIST_INDEX_PATH = "/dataset1/resourcelist-index.xml";
    private static final String RESOURCELIST_0 = "/dataset1/resourcelist_0000.xml";
    private static final String RESOURCELIST_1 = "/dataset1/resourcelist_0001.xml";

    @Test
    public void getResourceListIndex(){
        getMockServer()
                .when(HttpRequest.request()
                                .withMethod("GET")
                                .withPath(CAPABILITYLIST_PATH),
                        Times.exactly(1))

                .respond(HttpResponse.response()
                        .withStatusCode(200)
                        .withBody(createCapabilityList())
                );
        getMockServer()
                .when(HttpRequest.request()
                        .withMethod("GET")
                        .withPath(RESOURCELIST_INDEX_PATH),
                Times.exactly(1))

                .respond(HttpResponse.response()
                        .withStatusCode(200)
                        .withBody(createResourceListIndex())
                );

        ResourceListExplorer explorer = new ResourceListExplorer(getHttpclient(), getRsContext());
        ResultIndex index = new ResultIndex();
        explorer.explore(composeUri(CAPABILITYLIST_PATH), index);
        ResultIndexPivot pivot = new ResultIndexPivot(index);
        List<String> urlsets = pivot.listSitemapLocations(Capability.RESOURCELIST);

        assertThat(urlsets.size(), is(2));
        assertThat(urlsets, hasItems(composeUri(RESOURCELIST_0).toString(), composeUri(RESOURCELIST_1).toString()));

    }

    private String createCapabilityList() {
        return
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                        "<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\"\n" +
                        "        xmlns:rs=\"http://www.openarchives.org/rs/terms/\">\n" +
                        "  <rs:ln rel=\"describedby\"\n" +
                        "         href=\""+ composeUri("dataset1/info_about_set1_of_resources.xml") + "\"/>\n" +
                        "  <rs:ln rel=\"up\"\n" +
                        "         href=\""+ composeUri("dataset1/resourcesync_description.xml") + "\"/>\n" +
                        "  <rs:md capability=\"capabilitylist\"/>\n" +
                        "  <url>\n" +
                        "      <loc>" + composeUri("dataset1/resourcelist-index.xml") + "</loc>\n" +
                        "      <rs:md capability=\"resourcelist\"/>\n" +
                        "  </url>\n" +
                        "  <url>\n" +
                        "      <loc>" + composeUri("dataset1/changelist.xml") + "</loc>\n" +
                        "      <rs:md capability=\"changelist\"/>\n" +
                        "  </url>\n" +
                        "</urlset>\n";
    }

    private String createResourceListIndex() {
        return
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                        "<sitemapindex xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\"\n" +
                        "        xmlns:rs=\"http://www.openarchives.org/rs/terms/\">\n" +
                        "  <rs:ln rel=\"up\"\n" +
                        "         href=\"" + composeUri("dataset1/capabilitylist.xml") + "\"/>\n" +
                        "  <rs:md capability=\"resourcelist\"/>\n" +
                        "  <sitemap>\n" +
                        "      <loc>" + composeUri("dataset1/resourcelist_0000.xml") + "</loc>\n" +
                        "  </sitemap>\n" +
                        "  <sitemap>\n" +
                        "      <loc>" + composeUri("dataset1/resourcelist_0001.xml") + "</loc>\n" +
                        "  </sitemap>\n" +
                        "</sitemapindex>\n";
    }

    private String createResourceListPage() {
        return
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                        "<sitemaps xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\"\n" +
                        "        xmlns:rs=\"http://www.openarchives.org/rs/terms/\">\n" +
                        "  <rs:ln rel=\"up\"\n" +
                        "         href=\"http://example.com/dataset1/capabilitylist.xml\"/>\n" +
                        "  <rs:md capability=\"resourcelist\"/>\n" +
                        "  <sitemap>\n" +
                        "      <loc>http://example.com/dataset1/resource01.xml</loc>\n" +
                        "  </sitemap>\n" +
                        "  <sitemap>\n" +
                        "      <loc>http://example.com/dataset1/resource02.xml</loc>\n" +
                        "  </sitemap>\n" +
                        "</sitemaps>\n";
    }

}