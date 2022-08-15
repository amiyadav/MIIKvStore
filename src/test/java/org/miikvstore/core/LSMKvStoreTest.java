package org.miikvstore.core;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class LSMKvStoreTest {

    @Test
    public void set() throws IOException {
        KvStore kvStore = new LSMKvStore("/Users/amityadav/kvstore/db/", 4, 3);
//        for (int i = 0; i < 11; i++) {
//            kvStore.append(i + "", i + "");
//        }
//        for (int i = 0; i < 11; i++) {
//            assertEquals(i + "", kvStore.fetch(i + ""));
//        }
//        for (int i = 0; i < 11; i++) {
//            kvStore.delete(i + "");
//        }
//        for (int i = 0; i < 11; i++) {
//            assertNull(kvStore.fetch(i + ""));
//        }
//        kvStore.close();
//        kvStore = new LSMKvStore("/Users/amityadav/kvstore/db/", 4, 3);
        for (int i = 0; i < 11; i++) {
            assertNull(kvStore.fetch(i + ""));
        }
        kvStore.close();
    }
}
