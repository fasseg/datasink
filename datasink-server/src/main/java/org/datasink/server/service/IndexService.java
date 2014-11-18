package org.datasink.server.service;

import org.datasink.DataSetVersion;
import org.datasink.DataSet;

import java.io.IOException;

/**
 * Service for indexing and retrieval from the index
 * @author Frank Asseg
 */
public interface IndexService {
    void saveOrUpdate(DataSetVersion ds) throws IOException;

    DataSetVersion retrieve(final String id) throws IOException;

    DataSet retrieveDataset(final String id) throws IOException;

    void delete(final String id) throws IOException;

    boolean versionExists(String id, final int version) throws IOException;

    boolean exists(final String id) throws IOException;

    int lastVersionOf(String id) throws IOException;
}
