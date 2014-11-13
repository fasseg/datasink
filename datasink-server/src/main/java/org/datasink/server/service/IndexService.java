package org.datasink.server.service;

import org.datasink.Dataset;

import java.io.IOException;

/**
 * Service for indexing and retrieval from the index
 * @author Frank Asseg
 */
public interface IndexService {
    void saveOrUpdate(Dataset ds) throws IOException;

    Dataset retrieve(final String id, final int version) throws IOException;

    void delete(final String id, final int version) throws IOException;

    boolean versionExists(String id, final int version) throws IOException;

    boolean exists(final String id) throws IOException;
}
