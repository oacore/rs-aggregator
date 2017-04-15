package nl.knaw.dans.rs.aggregator.sync;

import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;

/**
 * Created on 2017-04-13 09:55.
 */
public class PathFinderTest {

  @Test
  public void testCreateSetPath() throws Exception {
    String[] expectations = {
      "http://zandbak02.dans.knaw.nl/ehri2/mdx/capabilitylist.xml",
      "http://ZAndbak02.dans.knaw.nl/ehri2/mdx/ls/resourcelist_0001.xml",
      "http://www.ZAndbak02.dans.knaw.nl/ehri2/rs/some/path/resource.ead",

      "http://www.ZANdbak02.dans.KNAW.nl/ehri2/..///mdy//capabilitylist.xml",
      "http://Zandbak02.dans.knaw.nl/mdy/resourcelist_0002.xml",
      "http://Zandbak02.dans.knaw.nl/resource.txt",

      "http://ZAndbak02.dans.knaw.nl:8080/ehri2/mdx/capabilitylist.xml", "" +
      "http://ZAndbak02.dans.knaw.nl:8080/ehri2/mdx/resourcelist_0003.xml",
      "http://zandbak02.dans.knaw.nl:8080/foo/bar/text.txt"
    };

    String baseDir = "target/test-output/pathmaster";
    if (new File(baseDir).exists()) {
      Path rootPath = Paths.get(baseDir);
      Files.walk(rootPath, FileVisitOption.FOLLOW_LINKS)
        .sorted(Comparator.reverseOrder())
        .map(Path::toFile)
        .forEach(File::delete);
    }

    for (int i=0; i < expectations.length; i += 3) {
      URI capaUri = URI.create(expectations[i]);

      PathFinder pm = new PathFinder(baseDir, capaUri);

      System.out.println(pm.getBaseDirectory());

      System.out.println(pm.getSetDirectory());
      assertThat(pm.getSetDirectory().mkdirs(), is(true));

      System.out.println(pm.getMetadataDirectory());
      assertThat(pm.getMetadataDirectory().mkdirs(), is(true));

      System.out.println(pm.getResourceDirectory());
      assertThat(pm.getResourceDirectory().mkdirs(), is(true));

      System.out.println(pm.getCapabilityListFile());

      URI mdUri = URI.create(expectations[i + 1]);
      System.out.println(pm.getMetadataFilePath(mdUri));

      URI rsUri = URI.create(expectations[i + 2]);
      System.out.println(pm.getResourceFilePath(rsUri));

      assertThat(pm.getCapabilityListFile(), equalTo(pm.getMetadataFilePath(capaUri)));

      System.out.println();

    }

  }

}