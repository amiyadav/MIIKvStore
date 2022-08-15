package org.miikvstore.core;

import java.io.Closeable;
import java.io.IOException;

/**
 * Key value store
 */
public interface KvStore extends Closeable {
    // append key-value with command
    void append(String key, String value) throws IOException;
    // fetch key
    String fetch(String key);
    // delete key  with marker/command
    void delete(String key);
}
