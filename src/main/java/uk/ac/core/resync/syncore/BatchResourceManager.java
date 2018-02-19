package uk.ac.core.resync.syncore;

import nl.knaw.dans.rs.aggregator.syncore.ResourceManager;

/**
 * Created by mc26486 on 16/01/2018.
 */
public interface BatchResourceManager extends ResourceManager {

    public boolean performBatchDownload();
}
