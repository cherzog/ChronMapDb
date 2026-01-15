package com.cherzog.chronmapdb;

import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Set;

/**
 * Helferklasse für vereinfachte Verwendung von ChronMapDb.
 * 
 * Diese Klasse bietet statische Utility-Methoden für häufige Operationen wie:
 * - Schnelle Erstellung von Standard-Datenbanken
 * - Verwaltung von benannten Instanzen
 * - Batch-Operationen
 * 
 * @author Christian Herzog
 */
public final class MapDbHelper {
    
    private static final Logger logger = LoggerFactory.getLogger(MapDbHelper.class);
    
    // Private Konstruktor verhindert Instanzierung
    private MapDbHelper() {
        throw new AssertionError("Utility-Klasse darf nicht instanziert werden");
    }
    
    /**
     * Erstellt eine einfache String-zu-String ChronMapDb mit Standardeinstellungen.
     * 
     * Diese Convenience-Methode ist ideal für einfache Anwendungsfälle:
     * - 10.000 erwartete Einträge
     * - 30 Sekunden Snapshot-Intervall
     * - 20 Bytes durchschnittliche Schlüsselgröße
     * - 100 Bytes durchschnittliche Wertgröße
     * 
     * @param name Der eindeutige Name der Datenbank (wird auch für Dateinamen verwendet)
     * @return Eine neue ChronMapDb-Instanz
     * @throws IOException Bei I/O-Fehlern
     * @throws IllegalArgumentException Wenn name null oder leer ist
     */
    public static ChronMapDb<String, String> createSimpleStringDb(String name) throws IOException {
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("Name darf nicht null oder leer sein");
        }
        
        logger.info("Erstelle einfache String-Datenbank '{}'", name);
        
        return new ChronMapDb.Builder<String, String>()
            .name(name)
            .types(String.class, String.class)
            .keySerializer(Serializer.STRING)
            .valueSerializer(Serializer.STRING)
            .build();
    }
    
    /**
     * Erstellt eine einfache typisierte ChronMapDb mit Standardeinstellungen.
     * 
     * Diese Methode vereinfacht die Erstellung von typisierten Datenbanken:
     * - 10.000 erwartete Einträge
     * - 30 Sekunden Snapshot-Intervall
     * - 20 Bytes durchschnittliche Schlüsselgröße
     * - 100 Bytes durchschnittliche Wertgröße
     * 
     * @param <K> Der Typ der Map-Schlüssel
     * @param <V> Der Typ der Map-Werte
     * @param name Der eindeutige Name der Datenbank
     * @param keyClass Die Klasse der Schlüssel
     * @param valueClass Die Klasse der Werte
     * @param keySerializer Der Serializer für Schlüssel
     * @param valueSerializer Der Serializer für Werte
     * @return Eine neue ChronMapDb-Instanz
     * @throws IOException Bei I/O-Fehlern
     * @throws IllegalArgumentException Wenn erforderliche Parameter null sind
     */
    public static <K, V> ChronMapDb<K, V> createSimpleDb(
            String name,
            Class<K> keyClass,
            Class<V> valueClass,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer) throws IOException {
        
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("Name darf nicht null oder leer sein");
        }
        if (keyClass == null) {
            throw new IllegalArgumentException("keyClass darf nicht null sein");
        }
        if (valueClass == null) {
            throw new IllegalArgumentException("valueClass darf nicht null sein");
        }
        if (keySerializer == null) {
            throw new IllegalArgumentException("keySerializer darf nicht null sein");
        }
        if (valueSerializer == null) {
            throw new IllegalArgumentException("valueSerializer darf nicht null sein");
        }
        
        logger.info("Erstelle typisierte Datenbank '{}' mit Typen {} -> {}", 
                    name, keyClass.getSimpleName(), valueClass.getSimpleName());
        
        return new ChronMapDb.Builder<K, V>()
            .name(name)
            .types(keyClass, valueClass)
            .keySerializer(keySerializer)
            .valueSerializer(valueSerializer)
            .build();
    }
    
    /**
     * Erstellt eine optimierte ChronMapDb für große Datenmengen.
     * 
     * Diese Methode ist für Anwendungsfälle mit vielen Einträgen optimiert:
     * - Konfigurierbare Anzahl erwarteter Einträge
     * - 60 Sekunden Snapshot-Intervall (weniger häufige Snapshots)
     * - Konfigurierbare durchschnittliche Größen
     * 
     * @param <K> Der Typ der Map-Schlüssel
     * @param <V> Der Typ der Map-Werte
     * @param name Der eindeutige Name der Datenbank
     * @param keyClass Die Klasse der Schlüssel
     * @param valueClass Die Klasse der Werte
     * @param keySerializer Der Serializer für Schlüssel
     * @param valueSerializer Der Serializer für Werte
     * @param expectedEntries Erwartete Anzahl von Einträgen
     * @param averageKeySize Durchschnittliche Schlüsselgröße in Bytes
     * @param averageValueSize Durchschnittliche Wertgröße in Bytes
     * @return Eine neue ChronMapDb-Instanz
     * @throws IOException Bei I/O-Fehlern
     * @throws IllegalArgumentException Wenn erforderliche Parameter ungültig sind
     */
    public static <K, V> ChronMapDb<K, V> createLargeDb(
            String name,
            Class<K> keyClass,
            Class<V> valueClass,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer,
            long expectedEntries,
            int averageKeySize,
            int averageValueSize) throws IOException {
        
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("Name darf nicht null oder leer sein");
        }
        if (keyClass == null || valueClass == null) {
            throw new IllegalArgumentException("Typ-Klassen dürfen nicht null sein");
        }
        if (keySerializer == null || valueSerializer == null) {
            throw new IllegalArgumentException("Serializer dürfen nicht null sein");
        }
        if (expectedEntries <= 0) {
            throw new IllegalArgumentException("Erwartete Einträge müssen größer als 0 sein");
        }
        if (averageKeySize <= 0 || averageValueSize <= 0) {
            throw new IllegalArgumentException("Durchschnittliche Größen müssen größer als 0 sein");
        }
        
        logger.info("Erstelle große Datenbank '{}' mit {} erwarteten Einträgen", name, expectedEntries);
        
        return new ChronMapDb.Builder<K, V>()
            .name(name)
            .types(keyClass, valueClass)
            .keySerializer(keySerializer)
            .valueSerializer(valueSerializer)
            .entries(expectedEntries)
            .averageKeySize(averageKeySize)
            .averageValueSize(averageValueSize)
            .snapshotIntervalSeconds(60)  // Weniger häufige Snapshots für große DBs
            .build();
    }
    
    /**
     * Erstellt eine ChronMapDb mit schnellen Snapshots für kritische Daten.
     * 
     * Diese Methode ist für Anwendungsfälle optimiert, bei denen Datenverlust
     * minimiert werden muss:
     * - 5 Sekunden Snapshot-Intervall (sehr häufige Snapshots)
     * - Geeignet für kritische Geschäftsdaten
     * - Höhere I/O-Last, dafür minimales Datenverlust-Fenster
     * 
     * @param <K> Der Typ der Map-Schlüssel
     * @param <V> Der Typ der Map-Werte
     * @param name Der eindeutige Name der Datenbank
     * @param keyClass Die Klasse der Schlüssel
     * @param valueClass Die Klasse der Werte
     * @param keySerializer Der Serializer für Schlüssel
     * @param valueSerializer Der Serializer für Werte
     * @return Eine neue ChronMapDb-Instanz
     * @throws IOException Bei I/O-Fehlern
     * @throws IllegalArgumentException Wenn erforderliche Parameter null sind
     */
    public static <K, V> ChronMapDb<K, V> createFastSnapshotDb(
            String name,
            Class<K> keyClass,
            Class<V> valueClass,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer) throws IOException {
        
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("Name darf nicht null oder leer sein");
        }
        if (keyClass == null || valueClass == null) {
            throw new IllegalArgumentException("Typ-Klassen dürfen nicht null sein");
        }
        if (keySerializer == null || valueSerializer == null) {
            throw new IllegalArgumentException("Serializer dürfen nicht null sein");
        }
        
        logger.info("Erstelle Fast-Snapshot-Datenbank '{}' mit 5s Intervall", name);
        
        return new ChronMapDb.Builder<K, V>()
            .name(name)
            .types(keyClass, valueClass)
            .keySerializer(keySerializer)
            .valueSerializer(valueSerializer)
            .snapshotIntervalSeconds(5)  // Sehr häufige Snapshots
            .build();
    }
    
    /**
     * Erstellt eine temporäre ChronMapDb ohne Namen (kein Singleton-Verhalten).
     * 
     * Diese Methode ist nützlich für Tests oder kurzlebige Datenbanken:
     * - Keine Singleton-Registry
     * - Temporäre Datei
     * - Wird bei jedem Aufruf neu erstellt
     * 
     * @param <K> Der Typ der Map-Schlüssel
     * @param <V> Der Typ der Map-Werte
     * @param keyClass Die Klasse der Schlüssel
     * @param valueClass Die Klasse der Werte
     * @param keySerializer Der Serializer für Schlüssel
     * @param valueSerializer Der Serializer für Werte
     * @return Eine neue temporäre ChronMapDb-Instanz
     * @throws IOException Bei I/O-Fehlern
     * @throws IllegalArgumentException Wenn erforderliche Parameter null sind
     */
    public static <K, V> ChronMapDb<K, V> createTemporaryDb(
            Class<K> keyClass,
            Class<V> valueClass,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer) throws IOException {
        
        if (keyClass == null || valueClass == null) {
            throw new IllegalArgumentException("Typ-Klassen dürfen nicht null sein");
        }
        if (keySerializer == null || valueSerializer == null) {
            throw new IllegalArgumentException("Serializer dürfen nicht null sein");
        }
        
        // Erstelle temporäre Datei in System-Temp-Verzeichnis
        // MapDB erstellt die Datei beim Öffnen, daher verwenden wir einen eindeutigen Namen
        File tempDir = new File(System.getProperty("java.io.tmpdir"));
        String fileName = "chronmapdb-" + System.currentTimeMillis() + "-" + 
                         System.nanoTime() + ".db";
        File tempFile = new File(tempDir, fileName);
        tempFile.deleteOnExit();
        
        logger.info("Erstelle temporäre Datenbank in {}", tempFile.getAbsolutePath());
        
        return new ChronMapDb.Builder<K, V>()
            .types(keyClass, valueClass)
            .mapDbFile(tempFile)
            .keySerializer(keySerializer)
            .valueSerializer(valueSerializer)
            .build();
    }
    
    /**
     * Gibt alle registrierten Namen von ChronMapDb-Instanzen zurück.
     * 
     * Diese Methode ist nützlich für Monitoring und Debugging, um zu sehen,
     * welche benannten Datenbank-Instanzen aktuell aktiv sind.
     * 
     * @return Set mit allen registrierten Datenbank-Namen
     */
    public static Set<String> getRegisteredInstanceNames() {
        // Zugriff auf das statische instances-Feld in ChronMapDb
        // Da es package-private ist, können wir direkt darauf zugreifen
        return Set.copyOf(ChronMapDb.instances.keySet());
    }
    
    /**
     * Prüft, ob eine benannte ChronMapDb-Instanz existiert.
     * 
     * @param name Der Name der zu prüfenden Instanz
     * @return true wenn eine Instanz mit diesem Namen existiert, sonst false
     * @throws IllegalArgumentException Wenn name null ist
     */
    public static boolean existsInstance(String name) {
        if (name == null) {
            throw new IllegalArgumentException("Name darf nicht null sein");
        }
        return ChronMapDb.instances.containsKey(name);
    }
    
    /**
     * Erzwingt einen Snapshot für alle registrierten ChronMapDb-Instanzen.
     * 
     * Diese Methode ist nützlich vor einem geplanten Herunterfahren oder
     * für manuelle Backup-Operationen.
     * 
     * @return Anzahl der Instanzen, für die ein Snapshot erstellt wurde
     */
    public static int snapshotAll() {
        logger.info("Erstelle Snapshots für alle registrierten Instanzen");
        
        int count = 0;
        for (ChronMapDb<?, ?> instance : ChronMapDb.instances.values()) {
            if (instance != null) {
                try {
                    instance.snapshot();
                    count++;
                } catch (Exception e) {
                    logger.error("Fehler beim Snapshot für Instanz '{}'", instance.getName(), e);
                }
            }
        }
        
        logger.info("Snapshots für {} Instanzen erstellt", count);
        return count;
    }
    
    /**
     * Schließt alle registrierten ChronMapDb-Instanzen.
     * 
     * Diese Methode ist nützlich beim Herunterfahren der Anwendung,
     * um sicherzustellen, dass alle Datenbanken sauber geschlossen werden.
     * 
     * <p><strong>Warnung:</strong> Nach Aufruf dieser Methode sind alle
     * registrierten Instanzen geschlossen und sollten nicht mehr verwendet werden.</p>
     * 
     * @return Anzahl der geschlossenen Instanzen
     */
    public static int closeAll() {
        logger.info("Schließe alle registrierten ChronMapDb-Instanzen");
        
        int count = 0;
        // Kopie erstellen, da close() die Instanz aus dem Registry entfernt
        var instancesCopy = Set.copyOf(ChronMapDb.instances.values());
        
        for (ChronMapDb<?, ?> instance : instancesCopy) {
            if (instance != null) {
                try {
                    String name = instance.getName();
                    instance.close();
                    logger.info("Instanz '{}' geschlossen", name);
                    count++;
                } catch (Exception e) {
                    logger.error("Fehler beim Schließen der Instanz", e);
                }
            }
        }
        
        logger.info("{} Instanzen geschlossen", count);
        return count;
    }
    
    /**
     * Gibt die Gesamtanzahl aller Einträge über alle registrierten Instanzen zurück.
     * 
     * Diese Methode ist nützlich für Monitoring und Statistiken.
     * 
     * @return Gesamtanzahl der Einträge
     */
    public static long getTotalEntryCount() {
        long total = 0;
        for (ChronMapDb<?, ?> instance : ChronMapDb.instances.values()) {
            if (instance != null) {
                total += instance.size();
            }
        }
        return total;
    }
    
    /**
     * Gibt Statistiken über alle registrierten ChronMapDb-Instanzen zurück.
     * 
     * @return String mit Statistik-Informationen
     */
    public static String getStatistics() {
        StringBuilder sb = new StringBuilder();
        sb.append("=== ChronMapDb Statistiken ===\n");
        sb.append("Anzahl Instanzen: ").append(ChronMapDb.instances.size()).append("\n");
        sb.append("Gesamtanzahl Einträge: ").append(getTotalEntryCount()).append("\n");
        sb.append("\nDetails:\n");
        
        for (var entry : ChronMapDb.instances.entrySet()) {
            ChronMapDb<?, ?> instance = entry.getValue();
            if (instance != null) {
                sb.append("  - ").append(entry.getKey())
                  .append(": ").append(instance.size())
                  .append(" Einträge\n");
            }
        }
        
        return sb.toString();
    }
}
