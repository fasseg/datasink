package org.datasink.server.service;

import org.datasink.Dataset;
import org.datasink.DatasetVersion;

import java.io.IOException;

/**
 * Service for indexing and retrieval from the index
 * @author Frank Asseg
 */
public interface IndexService {
    void saveOrUpdate(DatasetVersion version) throws IOException;

    DatasetVersion retrieveDatasetVersion(final String id) throws IOException;

    Dataset retrieveDataset(final String id) throws IOException;

    void delete(final String id) throws IOException;

    boolean versionExists(String id, final int version) throws IOException;

    boolean exists(final String id) throws IOException;

    DatasetVersion retrieveLatestDatasetVersion(String datasetId) throws IOException;
}
