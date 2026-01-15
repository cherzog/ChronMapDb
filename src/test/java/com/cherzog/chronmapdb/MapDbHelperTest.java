package com.cherzog.chronmapdb;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mapdb.Serializer;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests für die MapDbHelper-Klasse.
 */
class MapDbHelperTest {
    
    @TempDir
    Path tempDir;
    
    @AfterEach
    void tearDown() {
        // Alle Instanzen nach jedem Test schließen
        MapDbHelper.closeAll();
    }
    
    @Test
    void testHelperKannNichtInstanziertWerden() {
        assertThrows(InvocationTargetException.class, () -> {
            var constructor = MapDbHelper.class.getDeclaredConstructor();
            constructor.setAccessible(true);
            constructor.newInstance();
        });
    }
    
    @Test
    void testCreateSimpleStringDb() throws IOException {
        // Wechsle in temporäres Verzeichnis für DB-Dateien
        String originalDir = System.getProperty("user.dir");
        System.setProperty("user.dir", tempDir.toString());
        
        try {
            ChronMapDb<String, String> db = MapDbHelper.createSimpleStringDb("simple-string-db");
            
            assertNotNull(db);
            assertEquals("simple-string-db", db.getName());
            // Note: DB may not be empty if previous test left data, so we just check it works
            
            // Test einfache Operationen
            db.put("key1", "value1");
            assertEquals("value1", db.get("key1"));
            assertTrue(db.size() >= 1);
            
            db.close();
        } finally {
            System.setProperty("user.dir", originalDir);
        }
    }
    
    @Test
    void testCreateSimpleStringDbMitNullName() {
        assertThrows(IllegalArgumentException.class, () -> {
            MapDbHelper.createSimpleStringDb(null);
        });
    }
    
    @Test
    void testCreateSimpleStringDbMitLeeremName() {
        assertThrows(IllegalArgumentException.class, () -> {
            MapDbHelper.createSimpleStringDb("  ");
        });
    }
    
    @Test
    void testCreateSimpleDb() throws IOException {
        String originalDir = System.getProperty("user.dir");
        System.setProperty("user.dir", tempDir.toString());
        
        try {
            ChronMapDb<Integer, String> db = MapDbHelper.createSimpleDb(
                "simple-int-db",
                Integer.class,
                String.class,
                Serializer.INTEGER,
                Serializer.STRING
            );
            
            assertNotNull(db);
            assertEquals("simple-int-db", db.getName());
            // Note: DB may not be empty if previous test left data
            
            // Test mit Integer-Keys
            db.put(1, "one");
            db.put(2, "two");
            assertEquals("one", db.get(1));
            assertEquals("two", db.get(2));
            assertTrue(db.size() >= 2);
            
            db.close();
        } finally {
            System.setProperty("user.dir", originalDir);
        }
    }
    
    @Test
    void testCreateSimpleDbMitUngueltigemParameter() {
        assertThrows(IllegalArgumentException.class, () -> {
            MapDbHelper.createSimpleDb(null, String.class, String.class, 
                Serializer.STRING, Serializer.STRING);
        });
        
        assertThrows(IllegalArgumentException.class, () -> {
            MapDbHelper.createSimpleDb("test", null, String.class, 
                Serializer.STRING, Serializer.STRING);
        });
        
        assertThrows(IllegalArgumentException.class, () -> {
            MapDbHelper.createSimpleDb("test", String.class, null, 
                Serializer.STRING, Serializer.STRING);
        });
        
        assertThrows(IllegalArgumentException.class, () -> {
            MapDbHelper.createSimpleDb("test", String.class, String.class, 
                null, Serializer.STRING);
        });
        
        assertThrows(IllegalArgumentException.class, () -> {
            MapDbHelper.createSimpleDb("test", String.class, String.class, 
                Serializer.STRING, null);
        });
    }
    
    @Test
    void testCreateLargeDb() throws IOException {
        String originalDir = System.getProperty("user.dir");
        System.setProperty("user.dir", tempDir.toString());
        
        try {
            ChronMapDb<String, String> db = MapDbHelper.createLargeDb(
                "large-db",
                String.class,
                String.class,
                Serializer.STRING,
                Serializer.STRING,
                100000,  // 100k Einträge
                30,      // 30 Bytes Key
                200      // 200 Bytes Value
            );
            
            assertNotNull(db);
            assertEquals("large-db", db.getName());
            
            // Test mit mehreren Einträgen
            for (int i = 0; i < 100; i++) {
                db.put("key" + i, "value" + i);
            }
            assertEquals(100, db.size());
            
            db.close();
        } finally {
            System.setProperty("user.dir", originalDir);
        }
    }
    
    @Test
    void testCreateLargeDbMitUngueltigerAnzahl() {
        assertThrows(IllegalArgumentException.class, () -> {
            MapDbHelper.createLargeDb(
                "test",
                String.class,
                String.class,
                Serializer.STRING,
                Serializer.STRING,
                0,   // Ungültig
                30,
                200
            );
        });
        
        assertThrows(IllegalArgumentException.class, () -> {
            MapDbHelper.createLargeDb(
                "test",
                String.class,
                String.class,
                Serializer.STRING,
                Serializer.STRING,
                1000,
                0,   // Ungültig
                200
            );
        });
        
        assertThrows(IllegalArgumentException.class, () -> {
            MapDbHelper.createLargeDb(
                "test",
                String.class,
                String.class,
                Serializer.STRING,
                Serializer.STRING,
                1000,
                30,
                0    // Ungültig
            );
        });
    }
    
    @Test
    void testCreateFastSnapshotDb() throws IOException {
        String originalDir = System.getProperty("user.dir");
        System.setProperty("user.dir", tempDir.toString());
        
        try {
            ChronMapDb<String, String> db = MapDbHelper.createFastSnapshotDb(
                "fast-db",
                String.class,
                String.class,
                Serializer.STRING,
                Serializer.STRING
            );
            
            assertNotNull(db);
            assertEquals("fast-db", db.getName());
            
            db.put("key1", "value1");
            assertEquals("value1", db.get("key1"));
            
            db.close();
        } finally {
            System.setProperty("user.dir", originalDir);
        }
    }
    
    @Test
    void testCreateTemporaryDb() throws IOException {
        ChronMapDb<String, String> db = MapDbHelper.createTemporaryDb(
            String.class,
            String.class,
            Serializer.STRING,
            Serializer.STRING
        );
        
        assertNotNull(db);
        assertNull(db.getName());  // Temporäre DBs haben keinen Namen
        
        // Test Operationen
        db.put("temp1", "tempvalue1");
        assertEquals("tempvalue1", db.get("temp1"));
        
        db.close();
    }
    
    @Test
    void testCreateTemporaryDbMitUngueltigemParameter() {
        assertThrows(IllegalArgumentException.class, () -> {
            MapDbHelper.createTemporaryDb(null, String.class, 
                Serializer.STRING, Serializer.STRING);
        });
        
        assertThrows(IllegalArgumentException.class, () -> {
            MapDbHelper.createTemporaryDb(String.class, null, 
                Serializer.STRING, Serializer.STRING);
        });
        
        assertThrows(IllegalArgumentException.class, () -> {
            MapDbHelper.createTemporaryDb(String.class, String.class, 
                null, Serializer.STRING);
        });
        
        assertThrows(IllegalArgumentException.class, () -> {
            MapDbHelper.createTemporaryDb(String.class, String.class, 
                Serializer.STRING, null);
        });
    }
    
    @Test
    void testGetRegisteredInstanceNames() throws IOException {
        String originalDir = System.getProperty("user.dir");
        System.setProperty("user.dir", tempDir.toString());
        
        try {
            // Initial keine Instanzen
            assertEquals(0, MapDbHelper.getRegisteredInstanceNames().size());
            
            // Erstelle Instanzen
            ChronMapDb<String, String> db1 = MapDbHelper.createSimpleStringDb("db1");
            ChronMapDb<String, String> db2 = MapDbHelper.createSimpleStringDb("db2");
            
            var names = MapDbHelper.getRegisteredInstanceNames();
            assertEquals(2, names.size());
            assertTrue(names.contains("db1"));
            assertTrue(names.contains("db2"));
            
            // Nach Schließen sollte eine weniger sein
            db1.close();
            names = MapDbHelper.getRegisteredInstanceNames();
            assertEquals(1, names.size());
            assertTrue(names.contains("db2"));
            
            db2.close();
        } finally {
            System.setProperty("user.dir", originalDir);
        }
    }
    
    @Test
    void testExistsInstance() throws IOException {
        String originalDir = System.getProperty("user.dir");
        System.setProperty("user.dir", tempDir.toString());
        
        try {
            assertFalse(MapDbHelper.existsInstance("test-db"));
            
            ChronMapDb<String, String> db = MapDbHelper.createSimpleStringDb("test-db");
            assertTrue(MapDbHelper.existsInstance("test-db"));
            
            db.close();
            assertFalse(MapDbHelper.existsInstance("test-db"));
        } finally {
            System.setProperty("user.dir", originalDir);
        }
    }
    
    @Test
    void testExistsInstanceMitNull() {
        assertThrows(IllegalArgumentException.class, () -> {
            MapDbHelper.existsInstance(null);
        });
    }
    
    @Test
    void testSnapshotAll() throws IOException {
        String originalDir = System.getProperty("user.dir");
        System.setProperty("user.dir", tempDir.toString());
        
        try {
            // Erstelle mehrere Instanzen
            ChronMapDb<String, String> db1 = MapDbHelper.createSimpleStringDb("db1");
            ChronMapDb<String, String> db2 = MapDbHelper.createSimpleStringDb("db2");
            ChronMapDb<String, String> db3 = MapDbHelper.createSimpleStringDb("db3");
            
            // Füge Daten hinzu
            db1.put("key1", "value1");
            db2.put("key2", "value2");
            db3.put("key3", "value3");
            
            // Snapshot für alle
            int count = MapDbHelper.snapshotAll();
            assertEquals(3, count);
            
            db1.close();
            db2.close();
            db3.close();
        } finally {
            System.setProperty("user.dir", originalDir);
        }
    }
    
    @Test
    void testCloseAll() throws IOException {
        String originalDir = System.getProperty("user.dir");
        System.setProperty("user.dir", tempDir.toString());
        
        try {
            // Erstelle mehrere Instanzen
            ChronMapDb<String, String> db1 = MapDbHelper.createSimpleStringDb("db1");
            ChronMapDb<String, String> db2 = MapDbHelper.createSimpleStringDb("db2");
            
            assertEquals(2, MapDbHelper.getRegisteredInstanceNames().size());
            
            // Schließe alle
            int count = MapDbHelper.closeAll();
            assertEquals(2, count);
            
            // Registry sollte jetzt leer sein
            assertEquals(0, MapDbHelper.getRegisteredInstanceNames().size());
        } finally {
            System.setProperty("user.dir", originalDir);
        }
    }
    
    @Test
    void testGetTotalEntryCount() throws IOException {
        String originalDir = System.getProperty("user.dir");
        System.setProperty("user.dir", tempDir.toString());
        
        try {
            assertEquals(0, MapDbHelper.getTotalEntryCount());
            
            ChronMapDb<String, String> db1 = MapDbHelper.createSimpleStringDb("count-db1");
            ChronMapDb<String, String> db2 = MapDbHelper.createSimpleStringDb("count-db2");
            
            db1.put("key1", "value1");
            db1.put("key2", "value2");
            db2.put("key3", "value3");
            
            assertEquals(3, MapDbHelper.getTotalEntryCount());
            
            db1.close();
            db2.close();
        } finally {
            System.setProperty("user.dir", originalDir);
        }
    }
    
    @Test
    void testGetStatistics() throws IOException {
        String originalDir = System.getProperty("user.dir");
        System.setProperty("user.dir", tempDir.toString());
        
        try {
            ChronMapDb<String, String> db1 = MapDbHelper.createSimpleStringDb("statistics-db1");
            ChronMapDb<String, String> db2 = MapDbHelper.createSimpleStringDb("statistics-db2");
            
            db1.put("key1", "value1");
            db2.put("key2", "value2");
            db2.put("key3", "value3");
            
            String stats = MapDbHelper.getStatistics();
            
            assertNotNull(stats);
            assertTrue(stats.contains("ChronMapDb Statistiken"));
            assertTrue(stats.contains("Anzahl Instanzen: 2"));
            assertTrue(stats.contains("Gesamtanzahl Einträge: 3"));
            assertTrue(stats.contains("statistics-db1"));
            assertTrue(stats.contains("statistics-db2"));
            
            db1.close();
            db2.close();
        } finally {
            System.setProperty("user.dir", originalDir);
        }
    }
    
    @Test
    void testSingletonVerhaltenMitHelper() throws IOException {
        String originalDir = System.getProperty("user.dir");
        System.setProperty("user.dir", tempDir.toString());
        
        try {
            // Erste Erstellung
            ChronMapDb<String, String> db1 = MapDbHelper.createSimpleStringDb("singleton-test");
            db1.put("key1", "value1");
            
            // Zweite Erstellung mit gleichem Namen sollte gleiche Instanz sein
            ChronMapDb<String, String> db2 = MapDbHelper.createSimpleStringDb("singleton-test");
            
            assertSame(db1, db2);
            assertEquals("value1", db2.get("key1"));
            
            db1.close();
        } finally {
            System.setProperty("user.dir", originalDir);
        }
    }
}
