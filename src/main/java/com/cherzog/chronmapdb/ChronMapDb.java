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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
        
        logger.info("ChronMapDb initialisiert mit Snapshot-Intervall von {} Sekunden", snapshotIntervalSeconds);
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
     * Gibt die zugrunde liegende ChronicleMap zurück.
     * 
     * @return Die ChronicleMap
     */
    public ChronicleMap<K, V> getChronicleMap() {
        return chronicleMap;
    }
    
    @Override
    public void close() {
        logger.info("Schließe ChronMapDb...");
        
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
        
        logger.info("ChronMapDb geschlossen");
    }
    
    /**
     * Builder-Klasse zum Erstellen von ChronMapDb-Instanzen.
     * 
     * @param <K> Der Typ der Map-Schlüssel
     * @param <V> Der Typ der Map-Werte
     */
    public static class Builder<K, V> {
        private ChronicleMap<K, V> chronicleMap;
        private File mapDbFile;
        private String mapName = "chronmap";
        private long snapshotIntervalSeconds = 30; // Standard: 30 Sekunden
        private Serializer<K> keySerializer;
        private Serializer<V> valueSerializer;
        
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
         * @return Eine neue ChronMapDb-Instanz
         * @throws IOException Bei I/O-Fehlern
         * @throws IllegalStateException Wenn erforderliche Parameter fehlen
         */
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
            
            return new ChronMapDb<>(this);
        }
    }
}
