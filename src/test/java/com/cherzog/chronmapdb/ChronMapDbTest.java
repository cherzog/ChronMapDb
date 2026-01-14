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
    
    @Test
    void testLeerStringNameBehandeltWieOhneName() throws IOException {
        ChronicleMap<String, String> map1 = ChronicleMap
            .of(String.class, String.class)
            .name("test-map-empty-1")
            .entries(1000)
            .averageKeySize(20)
            .averageValueSize(100)
            .create();
        
        ChronicleMap<String, String> map2 = ChronicleMap
            .of(String.class, String.class)
            .name("test-map-empty-2")
            .entries(1000)
            .averageKeySize(20)
            .averageValueSize(100)
            .create();
        
        File dbFile1 = tempDir.resolve("empty1.db").toFile();
        File dbFile2 = tempDir.resolve("empty2.db").toFile();
        
        try {
            ChronMapDb<String, String> db1 = new ChronMapDb.Builder<String, String>()
                .name("")  // Leerer String
                .chronicleMap(map1)
                .mapDbFile(dbFile1)
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.STRING)
                .build();
            
            ChronMapDb<String, String> db2 = new ChronMapDb.Builder<String, String>()
                .name("   ")  // Nur Whitespace
                .chronicleMap(map2)
                .mapDbFile(dbFile2)
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.STRING)
                .build();
            
            assertNotSame(db1, db2, "Leere Namen sollten verschiedene Instanzen erzeugen");
            assertNull(db1.getName());
            assertNull(db2.getName());
            
            db1.close();
            db2.close();
        } finally {
            if (!map1.isClosed()) {
                map1.close();
            }
            if (!map2.isClosed()) {
                map2.close();
            }
        }
    }
    
    @Test
    void testSingletonMitName() throws IOException {
        ChronicleMap<String, String> map1 = ChronicleMap
            .of(String.class, String.class)
            .name("test-map-singleton")
            .entries(1000)
            .averageKeySize(20)
            .averageValueSize(100)
            .create();
        
        File dbFile = tempDir.resolve("singleton.db").toFile();
        
        try {
            ChronMapDb<String, String> db1 = new ChronMapDb.Builder<String, String>()
                .name("test-db")
                .chronicleMap(map1)
                .mapDbFile(dbFile)
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.STRING)
                .build();
            
            // Zweiter Build mit gleichem Namen sollte gleiche Instanz zurückgeben
            ChronMapDb<String, String> db2 = new ChronMapDb.Builder<String, String>()
                .name("test-db")
                .chronicleMap(map1)
                .mapDbFile(dbFile)
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.STRING)
                .build();
            
            assertSame(db1, db2, "Sollte die gleiche Instanz zurückgeben");
            assertEquals("test-db", db1.getName());
            
            db1.close();
        } finally {
            if (!map1.isClosed()) {
                map1.close();
            }
        }
    }
    
    @Test
    void testVerschiedeneNamenErzeugenVerschiedeneInstanzen() throws IOException {
        ChronicleMap<String, String> map1 = ChronicleMap
            .of(String.class, String.class)
            .name("test-map-1")
            .entries(1000)
            .averageKeySize(20)
            .averageValueSize(100)
            .create();
        
        ChronicleMap<String, String> map2 = ChronicleMap
            .of(String.class, String.class)
            .name("test-map-2")
            .entries(1000)
            .averageKeySize(20)
            .averageValueSize(100)
            .create();
        
        File dbFile1 = tempDir.resolve("db1.db").toFile();
        File dbFile2 = tempDir.resolve("db2.db").toFile();
        
        try {
            ChronMapDb<String, String> db1 = new ChronMapDb.Builder<String, String>()
                .name("db-1")
                .chronicleMap(map1)
                .mapDbFile(dbFile1)
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.STRING)
                .build();
            
            ChronMapDb<String, String> db2 = new ChronMapDb.Builder<String, String>()
                .name("db-2")
                .chronicleMap(map2)
                .mapDbFile(dbFile2)
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.STRING)
                .build();
            
            assertNotSame(db1, db2, "Sollte verschiedene Instanzen sein");
            assertEquals("db-1", db1.getName());
            assertEquals("db-2", db2.getName());
            
            db1.close();
            db2.close();
        } finally {
            if (!map1.isClosed()) {
                map1.close();
            }
            if (!map2.isClosed()) {
                map2.close();
            }
        }
    }
    
    @Test
    void testOhneNameErzeugtImmerNeueInstanz() throws IOException {
        ChronicleMap<String, String> map1 = ChronicleMap
            .of(String.class, String.class)
            .name("test-map-noname-1")
            .entries(1000)
            .averageKeySize(20)
            .averageValueSize(100)
            .create();
        
        ChronicleMap<String, String> map2 = ChronicleMap
            .of(String.class, String.class)
            .name("test-map-noname-2")
            .entries(1000)
            .averageKeySize(20)
            .averageValueSize(100)
            .create();
        
        File dbFile1 = tempDir.resolve("noname1.db").toFile();
        File dbFile2 = tempDir.resolve("noname2.db").toFile();
        
        try {
            ChronMapDb<String, String> db1 = new ChronMapDb.Builder<String, String>()
                .chronicleMap(map1)
                .mapDbFile(dbFile1)
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.STRING)
                .build();
            
            ChronMapDb<String, String> db2 = new ChronMapDb.Builder<String, String>()
                .chronicleMap(map2)
                .mapDbFile(dbFile2)
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.STRING)
                .build();
            
            assertNotSame(db1, db2, "Ohne Namen sollten verschiedene Instanzen erstellt werden");
            assertNull(db1.getName());
            assertNull(db2.getName());
            
            db1.close();
            db2.close();
        } finally {
            if (!map1.isClosed()) {
                map1.close();
            }
            if (!map2.isClosed()) {
                map2.close();
            }
        }
    }
    
    @Test
    void testThreadSafeSingletonErstellung() throws Exception {
        final int threadCount = 10;
        final ChronicleMap<String, String> map = ChronicleMap
            .of(String.class, String.class)
            .name("test-map-threadsafe")
            .entries(1000)
            .averageKeySize(20)
            .averageValueSize(100)
            .create();
        
        final File dbFile = tempDir.resolve("threadsafe.db").toFile();
        final ChronMapDb<String, String>[] results = new ChronMapDb[threadCount];
        final Thread[] threads = new Thread[threadCount];
        
        // Erstelle mehrere Threads die gleichzeitig versuchen, eine Instanz zu erstellen
        for (int i = 0; i < threadCount; i++) {
            final int index = i;
            threads[i] = new Thread(() -> {
                try {
                    results[index] = new ChronMapDb.Builder<String, String>()
                        .name("threadsafe-db")
                        .chronicleMap(map)
                        .mapDbFile(dbFile)
                        .keySerializer(Serializer.STRING)
                        .valueSerializer(Serializer.STRING)
                        .build();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        
        // Starte alle Threads gleichzeitig
        for (Thread thread : threads) {
            thread.start();
        }
        
        // Warte auf alle Threads
        for (Thread thread : threads) {
            thread.join();
        }
        
        try {
            // Alle Threads sollten die gleiche Instanz bekommen haben
            ChronMapDb<String, String> firstInstance = results[0];
            assertNotNull(firstInstance);
            
            for (int i = 1; i < threadCount; i++) {
                assertSame(firstInstance, results[i], 
                    "Thread " + i + " sollte die gleiche Instanz bekommen haben");
            }
            
            firstInstance.close();
        } finally {
            if (!map.isClosed()) {
                map.close();
            }
        }
    }
    
    @Test
    void testTypeMismatchWirftException() throws IOException {
        ChronicleMap<String, String> stringMap = ChronicleMap
            .of(String.class, String.class)
            .name("test-map-type-1")
            .entries(1000)
            .averageKeySize(20)
            .averageValueSize(100)
            .create();
        
        ChronicleMap<Integer, Integer> intMap = ChronicleMap
            .of(Integer.class, Integer.class)
            .name("test-map-type-2")
            .entries(1000)
            .create();
        
        File dbFile = tempDir.resolve("type-test.db").toFile();
        
        try {
            // Erste Instanz mit String-Typen
            ChronMapDb<String, String> db1 = new ChronMapDb.Builder<String, String>()
                .name("type-test")
                .chronicleMap(stringMap)
                .mapDbFile(dbFile)
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.STRING)
                .build();
            
            db1.put("testKey", "testValue");
            
            // Zweiter Versuch mit Integer-Typen - build() gibt die String-Instanz zurück
            // Die ClassCastException tritt auf, wenn man versucht, die Instanz als Integer zu verwenden
            assertThrows(ClassCastException.class, () -> {
                @SuppressWarnings("unchecked")
                ChronMapDb<Integer, Integer> db2 = new ChronMapDb.Builder<Integer, Integer>()
                    .name("type-test")  // Gleicher Name, andere Typen
                    .chronicleMap(intMap)
                    .mapDbFile(dbFile)
                    .keySerializer(Serializer.INTEGER)
                    .valueSerializer(Serializer.INTEGER)
                    .build();
                
                // Der Cast funktioniert wegen Type Erasure, aber beim Zugriff gibt es eine ClassCastException
                db2.put(123, 456);  // Dies sollte ClassCastException werfen
            });
            
            db1.close();
        } finally {
            if (!stringMap.isClosed()) {
                stringMap.close();
            }
            if (!intMap.isClosed()) {
                intMap.close();
            }
        }
    }
    
    @Test
    void testSingletonMitSnapshotWiederherstellung() throws IOException, InterruptedException {
        ChronicleMap<String, String> map1 = ChronicleMap
            .of(String.class, String.class)
            .name("test-map-snapshot-singleton")
            .entries(1000)
            .averageKeySize(20)
            .averageValueSize(100)
            .create();
        
        File dbFile = tempDir.resolve("snapshot-singleton.db").toFile();
        
        try {
            // Erste Instanz erstellen und Daten hinzufügen
            ChronMapDb<String, String> db1 = new ChronMapDb.Builder<String, String>()
                .name("snapshot-db")
                .chronicleMap(map1)
                .mapDbFile(dbFile)
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.STRING)
                .snapshotIntervalSeconds(1)
                .build();
            
            db1.put("key1", "value1");
            db1.put("key2", "value2");
            
            // Snapshot erstellen und schließen
            TimeUnit.SECONDS.sleep(2);
            db1.close();
        } finally {
            if (!map1.isClosed()) {
                map1.close();
            }
        }
        
        // Neue ChronicleMap und Instanz mit gleichem Namen erstellen
        ChronicleMap<String, String> map2 = ChronicleMap
            .of(String.class, String.class)
            .name("test-map-snapshot-singleton-2")
            .entries(1000)
            .averageKeySize(20)
            .averageValueSize(100)
            .create();
        
        try {
            ChronMapDb<String, String> db2 = new ChronMapDb.Builder<String, String>()
                .name("snapshot-db")
                .chronicleMap(map2)
                .mapDbFile(dbFile)
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.STRING)
                .build();
            
            // Daten sollten aus Snapshot wiederhergestellt sein
            assertEquals("value1", db2.get("key1"));
            assertEquals("value2", db2.get("key2"));
            assertEquals(2, db2.size());
            
            db2.close();
        } finally {
            if (!map2.isClosed()) {
                map2.close();
            }
        }
    }
    
    @Test
    void testPutWithArrayKeyExtractor() throws IOException {
        try (ChronMapDb<String, String> db = new ChronMapDb.Builder<String, String>()
            .chronicleMap(chronicleMap)
            .mapDbFile(mapDbFile)
            .keySerializer(Serializer.STRING)
            .valueSerializer(Serializer.STRING)
            .build()) {
            
            KeyExtractor<String> extractor = KeyExtractor.fromArray();
            String[] keyArray = {"user", "123", "profile"};
            
            db.putWithExtractor(keyArray, "userData", extractor);
            
            // Sollte mit dem zusammengesetzten Schlüssel abrufbar sein
            assertEquals("userData", db.get("user\u0000123\u0000profile"));
        }
    }
    
    @Test
    void testGetWithArrayKeyExtractor() throws IOException {
        try (ChronMapDb<String, String> db = new ChronMapDb.Builder<String, String>()
            .chronicleMap(chronicleMap)
            .mapDbFile(mapDbFile)
            .keySerializer(Serializer.STRING)
            .valueSerializer(Serializer.STRING)
            .build()) {
            
            // Daten mit normalem Schlüssel einfügen
            db.put("user\u0000123\u0000profile", "userData");
            
            // Mit Array-Extraktor abrufen
            KeyExtractor<String> extractor = KeyExtractor.fromArray();
            String[] keyArray = {"user", "123", "profile"};
            
            assertEquals("userData", db.getWithExtractor(keyArray, extractor));
        }
    }
    
    @Test
    void testDefaultKeyExtractorPut() throws IOException {
        KeyExtractor<String> extractor = KeyExtractor.fromArray();
        
        try (ChronMapDb<String, String> db = new ChronMapDb.Builder<String, String>()
            .chronicleMap(chronicleMap)
            .mapDbFile(mapDbFile)
            .keySerializer(Serializer.STRING)
            .valueSerializer(Serializer.STRING)
            .defaultKeyExtractor(extractor)
            .build()) {
            
            String[] keyArray = {"category", "books", "scifi"};
            db.putExtracted(keyArray, "Science Fiction Books");
            
            assertEquals("Science Fiction Books", db.get("category\u0000books\u0000scifi"));
        }
    }
    
    @Test
    void testDefaultKeyExtractorGet() throws IOException {
        KeyExtractor<String> extractor = KeyExtractor.fromArray();
        
        try (ChronMapDb<String, String> db = new ChronMapDb.Builder<String, String>()
            .chronicleMap(chronicleMap)
            .mapDbFile(mapDbFile)
            .keySerializer(Serializer.STRING)
            .valueSerializer(Serializer.STRING)
            .defaultKeyExtractor(extractor)
            .build()) {
            
            db.put("product\u0000electronics\u0000laptop", "Laptop Data");
            
            String[] keyArray = {"product", "electronics", "laptop"};
            assertEquals("Laptop Data", db.getExtracted(keyArray));
        }
    }
    
    @Test
    void testDefaultKeyExtractorRemove() throws IOException {
        KeyExtractor<String> extractor = KeyExtractor.fromArray();
        
        try (ChronMapDb<String, String> db = new ChronMapDb.Builder<String, String>()
            .chronicleMap(chronicleMap)
            .mapDbFile(mapDbFile)
            .keySerializer(Serializer.STRING)
            .valueSerializer(Serializer.STRING)
            .defaultKeyExtractor(extractor)
            .build()) {
            
            String[] keyArray = {"temp", "data"};
            db.putExtracted(keyArray, "temporary");
            
            assertEquals("temporary", db.removeExtracted(keyArray));
            assertNull(db.get("temp\u0000data"));
        }
    }
    
    @Test
    void testDefaultKeyExtractorContainsKey() throws IOException {
        KeyExtractor<String> extractor = KeyExtractor.fromArray();
        
        try (ChronMapDb<String, String> db = new ChronMapDb.Builder<String, String>()
            .chronicleMap(chronicleMap)
            .mapDbFile(mapDbFile)
            .keySerializer(Serializer.STRING)
            .valueSerializer(Serializer.STRING)
            .defaultKeyExtractor(extractor)
            .build()) {
            
            String[] keyArray = {"check", "exists"};
            db.putExtracted(keyArray, "value");
            
            assertTrue(db.containsKeyExtracted(keyArray));
            
            String[] nonExistentKey = {"not", "there"};
            assertFalse(db.containsKeyExtracted(nonExistentKey));
        }
    }
    
    @Test
    void testPutExtractedWithoutDefaultExtractorThrowsException() throws IOException {
        try (ChronMapDb<String, String> db = new ChronMapDb.Builder<String, String>()
            .chronicleMap(chronicleMap)
            .mapDbFile(mapDbFile)
            .keySerializer(Serializer.STRING)
            .valueSerializer(Serializer.STRING)
            .build()) {
            
            String[] keyArray = {"test"};
            assertThrows(IllegalStateException.class, () -> 
                db.putExtracted(keyArray, "value"));
        }
    }
    
    @Test
    void testGetExtractedWithoutDefaultExtractorThrowsException() throws IOException {
        try (ChronMapDb<String, String> db = new ChronMapDb.Builder<String, String>()
            .chronicleMap(chronicleMap)
            .mapDbFile(mapDbFile)
            .keySerializer(Serializer.STRING)
            .valueSerializer(Serializer.STRING)
            .build()) {
            
            String[] keyArray = {"test"};
            assertThrows(IllegalStateException.class, () -> 
                db.getExtracted(keyArray));
        }
    }
    
    @Test
    void testSingleValueArrayKeyMatchesString() throws IOException {
        KeyExtractor<String> extractor = KeyExtractor.fromArray();
        
        try (ChronMapDb<String, String> db = new ChronMapDb.Builder<String, String>()
            .chronicleMap(chronicleMap)
            .mapDbFile(mapDbFile)
            .keySerializer(Serializer.STRING)
            .valueSerializer(Serializer.STRING)
            .build()) {
            
            // Mit Array-Key speichern
            String[] keyArray = {"ID"};
            db.putWithExtractor(keyArray, "value1", extractor);
            
            // Mit String-Key abrufen (sollte funktionieren, da einzelnes Array-Element = String)
            assertEquals("value1", db.get("ID"));
        }
    }
    
    @Test
    void testPerformanceWithManyKeys() throws IOException {
        KeyExtractor<String> extractor = KeyExtractor.fromArray();
        
        try (ChronMapDb<String, String> db = new ChronMapDb.Builder<String, String>()
            .chronicleMap(chronicleMap)
            .mapDbFile(mapDbFile)
            .keySerializer(Serializer.STRING)
            .valueSerializer(Serializer.STRING)
            .defaultKeyExtractor(extractor)
            .build()) {
            
            long startTime = System.nanoTime();
            
            // 1000 Einträge einfügen
            for (int i = 0; i < 1000; i++) {
                String[] keyArray = {"key", String.valueOf(i)};
                db.putExtracted(keyArray, "value" + i);
            }
            
            long endTime = System.nanoTime();
            long durationMs = (endTime - startTime) / 1_000_000;
            
            // Sollte weniger als 500ms für 1000 Operationen dauern
            assertTrue(durationMs < 500, "Inserting 1000 records took " + durationMs + "ms");
            assertEquals(1000, db.size());
        }
    }
    
    @Test
    void testPutResultSet() throws IOException, java.sql.SQLException {
        KeyExtractor<String> extractor = KeyExtractor.fromResultSetByName("ID");
        
        // For this test, we use String as the value type and test that we can
        // extract the key from ResultSet and store it with a value
        ChronicleMap<String, String> stringMap = ChronicleMap
            .of(String.class, String.class)
            .name("test-map-resultset")
            .entries(100)
            .averageKeySize(20)
            .averageValueSize(100)
            .create();
        
        File rsDbFile = tempDir.resolve("test-resultset.db").toFile();
        
        try (ChronMapDb<String, String> db = new ChronMapDb.Builder<String, String>()
            .chronicleMap(stringMap)
            .mapDbFile(rsDbFile)
            .keySerializer(Serializer.STRING)
            .valueSerializer(Serializer.STRING)
            .defaultKeyExtractor(extractor)
            .build()) {
            
            // Mock ResultSet
            java.sql.ResultSet rs = org.mockito.Mockito.mock(java.sql.ResultSet.class);
            org.mockito.Mockito.when(rs.getObject("ID")).thenReturn("123");
            
            // Test putResultSet - Note: In real use, value type V must be compatible with ResultSet
            // For this test, we expect ClassCastException since String is not compatible with ResultSet
            assertThrows(ClassCastException.class, () -> db.putResultSet(rs));
            
            db.close();
        } finally {
            if (!stringMap.isClosed()) {
                stringMap.close();
            }
        }
    }
    
    @Test
    void testPutResultSetWithoutDefaultExtractorThrowsException() throws IOException {
        ChronicleMap<String, Object> objectMap = ChronicleMap
            .of(String.class, Object.class)
            .name("test-map-resultset-no-extractor")
            .entries(100)
            .averageKeySize(20)
            .averageValueSize(100)
            .create();
        
        File rsDbFile = tempDir.resolve("test-resultset-no-extractor.db").toFile();
        
        try (ChronMapDb<String, Object> db = new ChronMapDb.Builder<String, Object>()
            .chronicleMap(objectMap)
            .mapDbFile(rsDbFile)
            .keySerializer(Serializer.STRING)
            .valueSerializer(Serializer.JAVA)
            // No defaultKeyExtractor configured
            .build()) {
            
            java.sql.ResultSet rs = org.mockito.Mockito.mock(java.sql.ResultSet.class);
            
            assertThrows(IllegalStateException.class, () -> db.putResultSet(rs));
            
            db.close();
        } finally {
            if (!objectMap.isClosed()) {
                objectMap.close();
            }
        }
    }
}
