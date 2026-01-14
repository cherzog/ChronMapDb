package com.cherzog.chronmapdb;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mapdb.Serializer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests für die automatische Instanzierung von ChronicleMap und MapDb.
 */
class AutoInstantiationTest {
    
    @TempDir
    Path tempDir;
    
    @AfterEach
    void cleanup() {
        // Cleanup für Singleton-Tests
        ChronMapDb.clearAllInstances();
    }
    
    @Test
    void testAutoCreateChronicleMapAndMapDbFile() throws IOException {
        // Teste automatische Erstellung mit nur einem Namen
        ChronMapDb<String, String> db = new ChronMapDb.Builder<String, String>()
            .name("test-db")
            .types(String.class, String.class)
            .keySerializer(Serializer.STRING)
            .valueSerializer(Serializer.STRING)
            .build();
        
        try {
            assertNotNull(db);
            assertNotNull(db.getChronicleMap());
            
            // Teste grundlegende Operationen
            db.put("key1", "value1");
            assertEquals("value1", db.get("key1"));
            assertEquals(1, db.size());
        } finally {
            db.close();
            // Cleanup der erstellten Datei
            new File("test-db.db").delete();
        }
    }
    
    @Test
    void testAutoCreateChronicleMapWithCustomSettings() throws IOException {
        File dbFile = tempDir.resolve("custom.db").toFile();
        
        ChronMapDb<String, String> db = new ChronMapDb.Builder<String, String>()
            .name("custom-db")
            .types(String.class, String.class)
            .entries(5000)
            .averageKeySize(50)
            .averageValueSize(200)
            .mapDbFile(dbFile)
            .keySerializer(Serializer.STRING)
            .valueSerializer(Serializer.STRING)
            .build();
        
        try {
            assertNotNull(db);
            assertNotNull(db.getChronicleMap());
            
            // Teste grundlegende Operationen
            db.put("key1", "value1");
            assertEquals("value1", db.get("key1"));
        } finally {
            db.close();
        }
    }
    
    @Test
    void testAutoCreateMapDbFileFromName() throws IOException {
        // Stelle sicher, dass die Datei nicht existiert
        File expectedFile = new File("auto-mapdb-test.db");
        expectedFile.delete();
        
        ChronMapDb<String, String> db = new ChronMapDb.Builder<String, String>()
            .name("auto-mapdb-test")
            .types(String.class, String.class)
            .keySerializer(Serializer.STRING)
            .valueSerializer(Serializer.STRING)
            .build();
        
        try {
            assertNotNull(db);
            
            // Teste grundlegende Operationen
            db.put("key1", "value1");
            assertEquals("value1", db.get("key1"));
            
            // Prüfe, ob die Datei erstellt wurde
            assertTrue(expectedFile.exists() || expectedFile.getParentFile() != null);
        } finally {
            db.close();
            // Cleanup
            expectedFile.delete();
        }
    }
    
    @Test
    void testBackwardCompatibilityWithManualInstances() throws IOException {
        // Teste, dass die alte Verwendung immer noch funktioniert
        net.openhft.chronicle.map.ChronicleMap<String, String> chronicleMap = 
            net.openhft.chronicle.map.ChronicleMap
                .of(String.class, String.class)
                .name("manual-map")
                .entries(1000)
                .averageKeySize(20)
                .averageValueSize(100)
                .create();
        
        File mapDbFile = tempDir.resolve("manual.db").toFile();
        
        try {
            ChronMapDb<String, String> db = new ChronMapDb.Builder<String, String>()
                .chronicleMap(chronicleMap)
                .mapDbFile(mapDbFile)
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.STRING)
                .build();
            
            assertNotNull(db);
            assertSame(chronicleMap, db.getChronicleMap());
            
            // Teste grundlegende Operationen
            db.put("key1", "value1");
            assertEquals("value1", db.get("key1"));
            
            db.close();
        } finally {
            if (!chronicleMap.isClosed()) {
                chronicleMap.close();
            }
        }
    }
    
    @Test
    void testThrowsExceptionWhenNoNameAndNoMapDbFile() {
        assertThrows(IllegalStateException.class, () -> {
            new ChronMapDb.Builder<String, String>()
                .types(String.class, String.class)
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.STRING)
                .build();
        });
    }
    
    @Test
    void testThrowsExceptionWhenNoTypesAndNoChronicleMap() {
        assertThrows(IllegalStateException.class, () -> {
            new ChronMapDb.Builder<String, String>()
                .name("test")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.STRING)
                .build();
        });
    }
    
    @Test
    void testInvalidEntriesThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> {
            new ChronMapDb.Builder<String, String>()
                .entries(0);
        });
        
        assertThrows(IllegalArgumentException.class, () -> {
            new ChronMapDb.Builder<String, String>()
                .entries(-1);
        });
    }
    
    @Test
    void testInvalidAverageKeySizeThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> {
            new ChronMapDb.Builder<String, String>()
                .averageKeySize(0);
        });
        
        assertThrows(IllegalArgumentException.class, () -> {
            new ChronMapDb.Builder<String, String>()
                .averageKeySize(-1);
        });
    }
    
    @Test
    void testInvalidAverageValueSizeThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> {
            new ChronMapDb.Builder<String, String>()
                .averageValueSize(0);
        });
        
        assertThrows(IllegalArgumentException.class, () -> {
            new ChronMapDb.Builder<String, String>()
                .averageValueSize(-1);
        });
    }
}
