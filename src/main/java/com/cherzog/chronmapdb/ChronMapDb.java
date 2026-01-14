package com.cherzog.chronmapdb;

import net.openhft.chronicle.map.ChronicleMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * ChronMapDb kombiniert ChronicleMap mit automatischen Snapshots in MapDB.
 * 
 * Diese Klasse bietet eine In-Memory Map (ChronicleMap) mit automatischer
 * Persistierung der Daten in eine MapDB-Datei. Nach jeder Änderung wird
 * automatisch ein Snapshot erstellt (standardmäßig alle 30 Sekunden).
 * 
 * @param <K> Der Typ der Map-Schlüssel
 * @param <V> Der Typ der Map-Werte
 */
public class ChronMapDb<K, V> implements AutoCloseable {
    
    private static final Logger logger = LoggerFactory.getLogger(ChronMapDb.class);
    
    // Static registry for singleton instances by name
    private static final ConcurrentHashMap<String, ChronMapDb<?, ?>> instances = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, ReentrantLock> locks = new ConcurrentHashMap<>();
    
    private final String name;
    private final ChronicleMap<K, V> chronicleMap;
    private final DB mapDb;
    private final HTreeMap<K, V> mapDbMap;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean hasChanges;
    private final long snapshotIntervalSeconds;
    
    /**
     * Privater Konstruktor - verwenden Sie den Builder zum Erstellen von Instanzen.
     */
    private ChronMapDb(Builder<K, V> builder) throws IOException {
        this.name = builder.name;
        this.chronicleMap = builder.chronicleMap;
        this.snapshotIntervalSeconds = builder.snapshotIntervalSeconds;
        this.hasChanges = new AtomicBoolean(false);
        
        // MapDB für Snapshots initialisieren
        this.mapDb = DBMaker
            .fileDB(builder.mapDbFile)
            .transactionEnable()
            .make();
        
        this.mapDbMap = mapDb
            .hashMap(builder.mapName, builder.keySerializer, builder.valueSerializer)
            .createOrOpen();
        
        // Bestehende Daten aus MapDB in ChronicleMap laden
        ladeDatenAusMapDb();
        
        // Scheduler für automatische Snapshots starten
        this.scheduler = Executors.newScheduledThreadPool(1);
        starteSnapshotScheduler();
        
        logger.info("ChronMapDb '{}' initialisiert mit Snapshot-Intervall von {} Sekunden", name, snapshotIntervalSeconds);
    }
    
    /**
     * Lädt bestehende Daten aus der MapDB in die ChronicleMap.
     */
    private void ladeDatenAusMapDb() {
        int geladeneEintraege = 0;
        for (Map.Entry<K, V> entry : mapDbMap.entrySet()) {
            chronicleMap.put(entry.getKey(), entry.getValue());
            geladeneEintraege++;
        }
        logger.info("{} Einträge aus MapDB geladen", geladeneEintraege);
    }
    
    /**
     * Startet den Scheduler für automatische Snapshots.
     */
    private void starteSnapshotScheduler() {
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                if (hasChanges.get()) {
                    erstelleSnapshot();
                    hasChanges.set(false);
                }
            } catch (Exception e) {
                logger.error("Fehler beim Erstellen des Snapshots", e);
            }
        }, snapshotIntervalSeconds, snapshotIntervalSeconds, TimeUnit.SECONDS);
    }
    
    /**
     * Erstellt einen Snapshot der aktuellen ChronicleMap-Daten in MapDB.
     */
    private synchronized void erstelleSnapshot() {
        logger.debug("Erstelle Snapshot...");
        
        // Alle Daten in MapDB kopieren
        mapDbMap.clear();
        for (Map.Entry<K, V> entry : chronicleMap.entrySet()) {
            mapDbMap.put(entry.getKey(), entry.getValue());
        }
        
        // Änderungen in MapDB committen
        mapDb.commit();
        
        logger.info("Snapshot mit {} Einträgen erstellt", mapDbMap.size());
    }
    
    /**
     * Fügt ein Schlüssel-Wert-Paar in die Map ein.
     * 
     * @param key Der Schlüssel
     * @param value Der Wert
     * @return Der vorherige Wert oder null
     */
    public V put(K key, V value) {
        V result = chronicleMap.put(key, value);
        hasChanges.set(true);
        return result;
    }
    
    /**
     * Gibt den Wert für einen Schlüssel zurück.
     * 
     * @param key Der Schlüssel
     * @return Der Wert oder null
     */
    public V get(K key) {
        return chronicleMap.get(key);
    }
    
    /**
     * Entfernt einen Schlüssel aus der Map.
     * 
     * @param key Der zu entfernende Schlüssel
     * @return Der vorherige Wert oder null
     */
    public V remove(K key) {
        V result = chronicleMap.remove(key);
        if (result != null) {
            hasChanges.set(true);
        }
        return result;
    }
    
    /**
     * Gibt die Anzahl der Einträge in der Map zurück.
     * 
     * @return Anzahl der Einträge
     */
    public long size() {
        return chronicleMap.size();
    }
    
    /**
     * Prüft ob die Map leer ist.
     * 
     * @return true wenn leer, sonst false
     */
    public boolean isEmpty() {
        return chronicleMap.isEmpty();
    }
    
    /**
     * Prüft ob ein Schlüssel in der Map existiert.
     * 
     * @param key Der zu prüfende Schlüssel
     * @return true wenn der Schlüssel existiert
     */
    public boolean containsKey(K key) {
        return chronicleMap.containsKey(key);
    }
    
    /**
     * Löscht alle Einträge aus der Map.
     */
    public void clear() {
        chronicleMap.clear();
        hasChanges.set(true);
    }
    
    /**
     * Erzwingt einen sofortigen Snapshot.
     */
    public void snapshot() {
        erstelleSnapshot();
        hasChanges.set(false);
    }
    
    /**
     * Gibt den Namen dieser ChronMapDb-Instanz zurück.
     * 
     * @return Der Name oder null wenn kein Name gesetzt wurde
     */
    public String getName() {
        return name;
    }
    
    /**
     * Gibt die zugrunde liegende ChronicleMap zurück.
     * 
     * @return Die ChronicleMap
     */
    public ChronicleMap<K, V> getChronicleMap() {
        return chronicleMap;
    }
    
    @Override
    public void close() {
        logger.info("Schließe ChronMapDb '{}'...", name);
        
        // Letzten Snapshot erstellen wenn Änderungen vorliegen
        if (hasChanges.get()) {
            erstelleSnapshot();
        }
        
        // Scheduler herunterfahren
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        // MapDB schließen
        mapDb.close();
        
        // ChronicleMap schließen
        chronicleMap.close();
        
        // Instanz aus dem Registry entfernen
        if (name != null) {
            instances.remove(name);
            locks.remove(name);
        }
        
        logger.info("ChronMapDb '{}' geschlossen", name);
    }
    
    /**
     * Builder-Klasse zum Erstellen von ChronMapDb-Instanzen.
     * 
     * @param <K> Der Typ der Map-Schlüssel
     * @param <V> Der Typ der Map-Werte
     */
    public static class Builder<K, V> {
        private String name;
        private ChronicleMap<K, V> chronicleMap;
        private File mapDbFile;
        private String mapName = "chronmap";
        private long snapshotIntervalSeconds = 30; // Standard: 30 Sekunden
        private Serializer<K> keySerializer;
        private Serializer<V> valueSerializer;
        
        /**
         * Setzt den eindeutigen Namen der ChronMapDb-Instanz (optional).
         * Wenn ein Name gesetzt wird, wird die Instanz als Singleton behandelt:
         * Mehrere build()-Aufrufe mit dem gleichen Namen geben die gleiche Instanz zurück.
         * 
         * @param name Der eindeutige Name der Datenbank
         * @return Dieser Builder
         */
        public Builder<K, V> name(String name) {
            this.name = name;
            return this;
        }
        
        /**
         * Setzt die zu verwendende ChronicleMap (erforderlich).
         * 
         * @param chronicleMap Die ChronicleMap-Instanz
         * @return Dieser Builder
         */
        public Builder<K, V> chronicleMap(ChronicleMap<K, V> chronicleMap) {
            this.chronicleMap = chronicleMap;
            return this;
        }
        
        /**
         * Setzt die MapDB-Datei für Snapshots (erforderlich).
         * 
         * @param mapDbFile Die MapDB-Datei
         * @return Dieser Builder
         */
        public Builder<K, V> mapDbFile(File mapDbFile) {
            this.mapDbFile = mapDbFile;
            return this;
        }
        
        /**
         * Setzt den Namen der Map in MapDB (optional, Standard: "chronmap").
         * 
         * @param mapName Der Map-Name
         * @return Dieser Builder
         */
        public Builder<K, V> mapName(String mapName) {
            this.mapName = mapName;
            return this;
        }
        
        /**
         * Setzt das Snapshot-Intervall in Sekunden (optional, Standard: 30).
         * 
         * @param seconds Intervall in Sekunden
         * @return Dieser Builder
         */
        public Builder<K, V> snapshotIntervalSeconds(long seconds) {
            if (seconds <= 0) {
                throw new IllegalArgumentException("Snapshot-Intervall muss größer als 0 sein");
            }
            this.snapshotIntervalSeconds = seconds;
            return this;
        }
        
        /**
         * Setzt den Serializer für Schlüssel (erforderlich).
         * 
         * @param serializer Der Schlüssel-Serializer
         * @return Dieser Builder
         */
        public Builder<K, V> keySerializer(Serializer<K> serializer) {
            this.keySerializer = serializer;
            return this;
        }
        
        /**
         * Setzt den Serializer für Werte (erforderlich).
         * 
         * @param serializer Der Wert-Serializer
         * @return Dieser Builder
         */
        public Builder<K, V> valueSerializer(Serializer<V> serializer) {
            this.valueSerializer = serializer;
            return this;
        }
        
        /**
         * Erstellt die ChronMapDb-Instanz.
         * 
         * Wenn ein Name gesetzt wurde, wird die Instanz als Singleton behandelt:
         * - Beim ersten Aufruf wird eine neue Instanz erstellt und im Registry gespeichert
         * - Bei weiteren Aufrufen mit dem gleichen Namen wird die existierende Instanz zurückgegeben
         * - Thread-sicher: Parallele Aufrufe mit dem gleichen Namen warten aufeinander
         * 
         * @return Eine ChronMapDb-Instanz (neu oder existierend)
         * @throws IOException Bei I/O-Fehlern
         * @throws IllegalStateException Wenn erforderliche Parameter fehlen
         */
        @SuppressWarnings("unchecked")
        public ChronMapDb<K, V> build() throws IOException {
            if (chronicleMap == null) {
                throw new IllegalStateException("ChronicleMap muss gesetzt werden");
            }
            if (mapDbFile == null) {
                throw new IllegalStateException("MapDB-Datei muss gesetzt werden");
            }
            if (keySerializer == null) {
                throw new IllegalStateException("Key-Serializer muss gesetzt werden");
            }
            if (valueSerializer == null) {
                throw new IllegalStateException("Value-Serializer muss gesetzt werden");
            }
            
            // Wenn kein Name gesetzt ist, erstelle eine neue Instanz ohne Singleton-Verhalten
            if (name == null || name.trim().isEmpty()) {
                return new ChronMapDb<>(this);
            }
            
            // Hole oder erstelle ein Lock für diesen Namen
            ReentrantLock lock = locks.computeIfAbsent(name, k -> new ReentrantLock());
            
            // Synchronisiere auf dem Lock für diesen Namen
            lock.lock();
            try {
                // Prüfe ob bereits eine Instanz mit diesem Namen existiert
                ChronMapDb<?, ?> existing = instances.get(name);
                
                if (existing != null) {
                    logger.info("Gebe existierende ChronMapDb-Instanz '{}' zurück", name);
                    return (ChronMapDb<K, V>) existing;
                }
                
                // Erstelle neue Instanz
                logger.info("Erstelle neue ChronMapDb-Instanz '{}'", name);
                ChronMapDb<K, V> newInstance = new ChronMapDb<>(this);
                
                // Speichere im Registry
                instances.put(name, newInstance);
                
                return newInstance;
            } finally {
                lock.unlock();
            }
        }
    }
}
