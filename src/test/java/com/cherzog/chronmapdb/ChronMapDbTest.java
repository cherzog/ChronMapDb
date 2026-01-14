package com.cherzog.chronmapdb;

import net.openhft.chronicle.map.ChronicleMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mapdb.Serializer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests für die ChronMapDb-Klasse.
 */
class ChronMapDbTest {
    
    @TempDir
    Path tempDir;
    
    private ChronicleMap<String, String> chronicleMap;
    private File mapDbFile;
    
    @BeforeEach
    void setUp() throws IOException {
        // ChronicleMap für Tests erstellen
        chronicleMap = ChronicleMap
            .of(String.class, String.class)
            .name("test-map")
            .entries(1000)
            .averageKeySize(20)
            .averageValueSize(100)
            .create();
        
        // MapDB-Datei im temporären Verzeichnis
        mapDbFile = tempDir.resolve("test.db").toFile();
    }
    
    @AfterEach
    void tearDown() {
        if (chronicleMap != null && !chronicleMap.isClosed()) {
            chronicleMap.close();
        }
    }
    
    @Test
    void testBuilderErstelltInstanz() throws IOException {
        try (ChronMapDb<String, String> db = new ChronMapDb.Builder<String, String>()
            .chronicleMap(chronicleMap)
            .mapDbFile(mapDbFile)
            .keySerializer(Serializer.STRING)
            .valueSerializer(Serializer.STRING)
            .build()) {
            
            assertNotNull(db);
            assertTrue(db.isEmpty());
        }
    }
    
    @Test
    void testBuilderMitCustomSnapshotIntervall() throws IOException {
        try (ChronMapDb<String, String> db = new ChronMapDb.Builder<String, String>()
            .chronicleMap(chronicleMap)
            .mapDbFile(mapDbFile)
            .keySerializer(Serializer.STRING)
            .valueSerializer(Serializer.STRING)
            .snapshotIntervalSeconds(10)
            .build()) {
            
            assertNotNull(db);
        }
    }
    
    @Test
    void testBuilderWirftExceptionOhneChronicleMap() {
        assertThrows(IllegalStateException.class, () -> {
            new ChronMapDb.Builder<String, String>()
                .mapDbFile(mapDbFile)
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.STRING)
                .build();
        });
    }
    
    @Test
    void testBuilderWirftExceptionOhneMapDbFile() {
        assertThrows(IllegalStateException.class, () -> {
            new ChronMapDb.Builder<String, String>()
                .chronicleMap(chronicleMap)
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.STRING)
                .build();
        });
    }
    
    @Test
    void testBuilderWirftExceptionOhneKeySerializer() {
        assertThrows(IllegalStateException.class, () -> {
            new ChronMapDb.Builder<String, String>()
                .chronicleMap(chronicleMap)
                .mapDbFile(mapDbFile)
                .valueSerializer(Serializer.STRING)
                .build();
        });
    }
    
    @Test
    void testBuilderWirftExceptionOhneValueSerializer() {
        assertThrows(IllegalStateException.class, () -> {
            new ChronMapDb.Builder<String, String>()
                .chronicleMap(chronicleMap)
                .mapDbFile(mapDbFile)
                .keySerializer(Serializer.STRING)
                .build();
        });
    }
    
    @Test
    void testBuilderWirftExceptionBeiUngueltigemIntervall() {
        assertThrows(IllegalArgumentException.class, () -> {
            new ChronMapDb.Builder<String, String>()
                .chronicleMap(chronicleMap)
                .mapDbFile(mapDbFile)
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.STRING)
                .snapshotIntervalSeconds(0)
                .build();
        });
    }
    
    @Test
    void testPutUndGet() throws IOException {
        try (ChronMapDb<String, String> db = new ChronMapDb.Builder<String, String>()
            .chronicleMap(chronicleMap)
            .mapDbFile(mapDbFile)
            .keySerializer(Serializer.STRING)
            .valueSerializer(Serializer.STRING)
            .build()) {
            
            db.put("key1", "value1");
            assertEquals("value1", db.get("key1"));
            assertEquals(1, db.size());
        }
    }
    
    @Test
    void testPutUeberschreibtWert() throws IOException {
        try (ChronMapDb<String, String> db = new ChronMapDb.Builder<String, String>()
            .chronicleMap(chronicleMap)
            .mapDbFile(mapDbFile)
            .keySerializer(Serializer.STRING)
            .valueSerializer(Serializer.STRING)
            .build()) {
            
            db.put("key1", "value1");
            String altWert = db.put("key1", "value2");
            
            assertEquals("value1", altWert);
            assertEquals("value2", db.get("key1"));
            assertEquals(1, db.size());
        }
    }
    
    @Test
    void testRemove() throws IOException {
        try (ChronMapDb<String, String> db = new ChronMapDb.Builder<String, String>()
            .chronicleMap(chronicleMap)
            .mapDbFile(mapDbFile)
            .keySerializer(Serializer.STRING)
            .valueSerializer(Serializer.STRING)
            .build()) {
            
            db.put("key1", "value1");
            String entfernterWert = db.remove("key1");
            
            assertEquals("value1", entfernterWert);
            assertNull(db.get("key1"));
            assertTrue(db.isEmpty());
        }
    }
    
    @Test
    void testContainsKey() throws IOException {
        try (ChronMapDb<String, String> db = new ChronMapDb.Builder<String, String>()
            .chronicleMap(chronicleMap)
            .mapDbFile(mapDbFile)
            .keySerializer(Serializer.STRING)
            .valueSerializer(Serializer.STRING)
            .build()) {
            
            db.put("key1", "value1");
            assertTrue(db.containsKey("key1"));
            assertFalse(db.containsKey("key2"));
        }
    }
    
    @Test
    void testClear() throws IOException {
        try (ChronMapDb<String, String> db = new ChronMapDb.Builder<String, String>()
            .chronicleMap(chronicleMap)
            .mapDbFile(mapDbFile)
            .keySerializer(Serializer.STRING)
            .valueSerializer(Serializer.STRING)
            .build()) {
            
            db.put("key1", "value1");
            db.put("key2", "value2");
            
            db.clear();
            
            assertTrue(db.isEmpty());
            assertEquals(0, db.size());
        }
    }
    
    @Test
    void testSnapshotWirdErstellt() throws IOException, InterruptedException {
        try (ChronMapDb<String, String> db = new ChronMapDb.Builder<String, String>()
            .chronicleMap(chronicleMap)
            .mapDbFile(mapDbFile)
            .keySerializer(Serializer.STRING)
            .valueSerializer(Serializer.STRING)
            .snapshotIntervalSeconds(1) // Kurzes Intervall für Tests
            .build()) {
            
            db.put("key1", "value1");
            db.put("key2", "value2");
            
            // Auf automatischen Snapshot warten
            TimeUnit.SECONDS.sleep(2);
        }
        
        // Neue Instanz erstellen um Daten aus MapDB zu laden
        ChronicleMap<String, String> neueChronicleMap = ChronicleMap
            .of(String.class, String.class)
            .name("test-map-2")
            .entries(1000)
            .averageKeySize(20)
            .averageValueSize(100)
            .create();
        
        try (ChronMapDb<String, String> db2 = new ChronMapDb.Builder<String, String>()
            .chronicleMap(neueChronicleMap)
            .mapDbFile(mapDbFile)
            .keySerializer(Serializer.STRING)
            .valueSerializer(Serializer.STRING)
            .build()) {
            
            // Daten sollten aus MapDB geladen worden sein
            assertEquals("value1", db2.get("key1"));
            assertEquals("value2", db2.get("key2"));
            assertEquals(2, db2.size());
        } finally {
            if (!neueChronicleMap.isClosed()) {
                neueChronicleMap.close();
            }
        }
    }
    
    @Test
    void testManuellerSnapshot() throws IOException {
        try (ChronMapDb<String, String> db = new ChronMapDb.Builder<String, String>()
            .chronicleMap(chronicleMap)
            .mapDbFile(mapDbFile)
            .keySerializer(Serializer.STRING)
            .valueSerializer(Serializer.STRING)
            .build()) {
            
            db.put("key1", "value1");
            db.snapshot(); // Manueller Snapshot
        }
        
        // Neue Instanz erstellen um Daten aus MapDB zu laden
        ChronicleMap<String, String> neueChronicleMap = ChronicleMap
            .of(String.class, String.class)
            .name("test-map-3")
            .entries(1000)
            .averageKeySize(20)
            .averageValueSize(100)
            .create();
        
        try (ChronMapDb<String, String> db2 = new ChronMapDb.Builder<String, String>()
            .chronicleMap(neueChronicleMap)
            .mapDbFile(mapDbFile)
            .keySerializer(Serializer.STRING)
            .valueSerializer(Serializer.STRING)
            .build()) {
            
            assertEquals("value1", db2.get("key1"));
        } finally {
            if (!neueChronicleMap.isClosed()) {
                neueChronicleMap.close();
            }
        }
    }
    
    @Test
    void testGetChronicleMap() throws IOException {
        try (ChronMapDb<String, String> db = new ChronMapDb.Builder<String, String>()
            .chronicleMap(chronicleMap)
            .mapDbFile(mapDbFile)
            .keySerializer(Serializer.STRING)
            .valueSerializer(Serializer.STRING)
            .build()) {
            
            assertSame(chronicleMap, db.getChronicleMap());
        }
    }
    
    @Test
    void testCloseErstelltLetztenSnapshot() throws IOException {
        try (ChronMapDb<String, String> db = new ChronMapDb.Builder<String, String>()
            .chronicleMap(chronicleMap)
            .mapDbFile(mapDbFile)
            .keySerializer(Serializer.STRING)
            .valueSerializer(Serializer.STRING)
            .snapshotIntervalSeconds(60) // Langes Intervall
            .build()) {
            
            db.put("key1", "value1");
            // close() wird automatisch aufgerufen
        }
        
        // Neue Instanz erstellen um zu prüfen ob Snapshot erstellt wurde
        ChronicleMap<String, String> neueChronicleMap = ChronicleMap
            .of(String.class, String.class)
            .name("test-map-4")
            .entries(1000)
            .averageKeySize(20)
            .averageValueSize(100)
            .create();
        
        try (ChronMapDb<String, String> db2 = new ChronMapDb.Builder<String, String>()
            .chronicleMap(neueChronicleMap)
            .mapDbFile(mapDbFile)
            .keySerializer(Serializer.STRING)
            .valueSerializer(Serializer.STRING)
            .build()) {
            
            assertEquals("value1", db2.get("key1"));
        } finally {
            if (!neueChronicleMap.isClosed()) {
                neueChronicleMap.close();
            }
        }
    }
}
