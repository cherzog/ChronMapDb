# ChronMapDb

Eine Java 17-Bibliothek, die ChronicleMap mit automatischen MapDB-Snapshots kombiniert.

## Überblick

ChronMapDb bietet eine leistungsstarke In-Memory-Map (ChronicleMap) mit automatischer Persistierung der Daten in eine MapDB-Datei. Nach jeder Änderung wird automatisch ein Snapshot erstellt (standardmäßig alle 30 Sekunden, konfigurierbar über den Builder).

## Features

- **Schnelle In-Memory-Operationen** mit ChronicleMap
- **Automatische Persistierung** der Daten in MapDB
- **Konfigurierbares Snapshot-Intervall** (Standard: 30 Sekunden)
- **Builder-Pattern** für einfache Konfiguration
- **Automatisches Laden** bestehender Daten beim Start
- **Thread-sicher** und produktionsreif
- **Java 17** mit modernen Java-Features

## Voraussetzungen

- Java 17 oder höher
- Maven 3.6 oder höher

## Installation

Fügen Sie folgende Dependency zu Ihrer `pom.xml` hinzu:

```xml
<dependency>
    <groupId>com.cherzog</groupId>
    <artifactId>chronmapdb</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

## Verwendung

### Grundlegende Verwendung

```java
import com.cherzog.chronmapdb.ChronMapDb;
import net.openhft.chronicle.map.ChronicleMap;
import org.mapdb.Serializer;
import java.io.File;

// ChronicleMap erstellen
ChronicleMap<String, String> chronicleMap = ChronicleMap
    .of(String.class, String.class)
    .name("meine-map")
    .entries(10000)
    .averageKeySize(20)
    .averageValueSize(100)
    .create();

// ChronMapDb mit Standard-Einstellungen erstellen (30 Sekunden Snapshot-Intervall)
try (ChronMapDb<String, String> db = new ChronMapDb.Builder<String, String>()
        .chronicleMap(chronicleMap)
        .mapDbFile(new File("daten.db"))
        .keySerializer(Serializer.STRING)
        .valueSerializer(Serializer.STRING)
        .build()) {
    
    // Daten hinzufügen
    db.put("schluessel1", "wert1");
    db.put("schluessel2", "wert2");
    
    // Daten abrufen
    String wert = db.get("schluessel1");
    
    // Daten entfernen
    db.remove("schluessel1");
    
    // Größe abfragen
    long anzahl = db.size();
    
    // Manueller Snapshot (optional)
    db.snapshot();
}
```

### Konfiguration des Snapshot-Intervalls

```java
try (ChronMapDb<String, String> db = new ChronMapDb.Builder<String, String>()
        .chronicleMap(chronicleMap)
        .mapDbFile(new File("daten.db"))
        .keySerializer(Serializer.STRING)
        .valueSerializer(Serializer.STRING)
        .snapshotIntervalSeconds(60)  // Snapshot alle 60 Sekunden
        .build()) {
    
    // Ihre Code hier...
}
```

### Verwendung mit benutzerdefinierten Typen

```java
// Für Integer-Keys und String-Values
ChronicleMap<Integer, String> intMap = ChronicleMap
    .of(Integer.class, String.class)
    .name("int-map")
    .entries(10000)
    .create();

try (ChronMapDb<Integer, String> db = new ChronMapDb.Builder<Integer, String>()
        .chronicleMap(intMap)
        .mapDbFile(new File("int-daten.db"))
        .keySerializer(Serializer.INTEGER)
        .valueSerializer(Serializer.STRING)
        .mapName("meine-int-map")
        .build()) {
    
    db.put(1, "erster Wert");
    db.put(2, "zweiter Wert");
}
```

## API-Dokumentation

### ChronMapDb-Methoden

- `put(K key, V value)` - Fügt ein Schlüssel-Wert-Paar hinzu
- `get(K key)` - Gibt den Wert für einen Schlüssel zurück
- `remove(K key)` - Entfernt einen Schlüssel
- `size()` - Gibt die Anzahl der Einträge zurück
- `isEmpty()` - Prüft ob die Map leer ist
- `containsKey(K key)` - Prüft ob ein Schlüssel existiert
- `clear()` - Löscht alle Einträge
- `snapshot()` - Erzwingt einen sofortigen Snapshot
- `getChronicleMap()` - Gibt die zugrunde liegende ChronicleMap zurück
- `close()` - Schließt die Ressourcen (erstellt letzten Snapshot)

### Builder-Optionen

- `chronicleMap(ChronicleMap<K, V>)` - **Erforderlich**: Die zu verwendende ChronicleMap
- `mapDbFile(File)` - **Erforderlich**: Die MapDB-Datei für Snapshots
- `keySerializer(Serializer<K>)` - **Erforderlich**: Serializer für Schlüssel
- `valueSerializer(Serializer<V>)` - **Erforderlich**: Serializer für Werte
- `snapshotIntervalSeconds(long)` - Optional: Snapshot-Intervall (Standard: 30)
- `mapName(String)` - Optional: Name der Map in MapDB (Standard: "chronmap")

## Projekt bauen

```bash
# Projekt kompilieren
mvn clean compile

# Tests ausführen
mvn test

# JAR-Datei erstellen
mvn package
```

## Funktionsweise

1. **Initialisierung**: Beim Start werden bestehende Daten aus der MapDB in die ChronicleMap geladen
2. **Operationen**: Alle Lese- und Schreiboperationen erfolgen auf der schnellen In-Memory ChronicleMap
3. **Änderungsverfolgung**: Bei jeder Änderung (put, remove, clear) wird ein Flag gesetzt
4. **Automatische Snapshots**: Ein Scheduler prüft im konfigurierten Intervall, ob Änderungen vorliegen
5. **Snapshot-Erstellung**: Bei Änderungen werden alle Daten in die MapDB kopiert und committed
6. **Sauberes Herunterfahren**: Beim Schließen wird ein letzter Snapshot erstellt

## Vorteile

- **Performance**: ChronicleMap bietet sehr schnelle In-Memory-Operationen
- **Persistenz**: MapDB sorgt für dauerhafte Speicherung der Daten
- **Zuverlässigkeit**: Automatische Snapshots verhindern Datenverlust
- **Flexibilität**: Konfigurierbares Snapshot-Intervall je nach Anforderung
- **Einfachheit**: Builder-Pattern macht die Verwendung sehr einfach

## Lizenz

MIT License

## Autor

Christian Herzog