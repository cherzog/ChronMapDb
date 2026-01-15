# ChronMapDb

Eine Java 21-Bibliothek, die ChronicleMap mit automatischen MapDB-Snapshots kombiniert.

## Überblick

ChronMapDb bietet eine leistungsstarke In-Memory-Map (ChronicleMap) mit automatischer Persistierung der Daten in eine MapDB-Datei. Sobald Änderungen vorliegen, wird periodisch ein Snapshot erstellt (standardmäßig alle 30 Sekunden, konfigurierbar über den Builder). Das bedeutet: Werden innerhalb des Snapshot-Intervalls viele Datensätze geschrieben (z.B. 1.000.000 Einträge in 30 Sekunden), erfolgt nur ein Snapshot. Im Falle eines unerwarteten Neustarts können maximal die Änderungen des letzten Intervalls (Standard: 30 Sekunden) verloren gehen.

## Features

- **Schnelle In-Memory-Operationen** mit ChronicleMap
- **Automatische Persistierung** der Daten in MapDB
- **Vereinfachte Verwendung** - ChronicleMap und MapDB werden automatisch erstellt
- **MapDbHelper-Utility-Klasse** - Praktische Hilfsmethoden für häufige Anwendungsfälle
- **Konfigurierbares Snapshot-Intervall** (Standard: 30 Sekunden)
- **Builder-Pattern** für einfache Konfiguration
- **Automatisches Laden** bestehender Daten beim Start
- **Singleton-Unterstützung** - Benannte Instanzen werden wiederverwendet
- **Thread-sicher** und produktionsreif
- **Java 21** mit modernen Java-Features

## Voraussetzungen

- Java 21 oder höher
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

### Einfachste Verwendung mit MapDbHelper (empfohlen)

Die MapDbHelper-Klasse bietet praktische Utility-Methoden für häufige Anwendungsfälle:

```java
import com.cherzog.chronmapdb.MapDbHelper;

// Einfache String-zu-String Datenbank mit Standardeinstellungen
try (ChronMapDb<String, String> db = MapDbHelper.createSimpleStringDb("meine-db")) {
    db.put("schluessel1", "wert1");
    db.put("schluessel2", "wert2");
    String wert = db.get("schluessel1");
}

// Typisierte Datenbank (z.B. Integer -> String)
try (ChronMapDb<Integer, String> db = MapDbHelper.createSimpleDb(
        "user-db",
        Integer.class,
        String.class,
        Serializer.INTEGER,
        Serializer.STRING)) {
    
    db.put(1, "Alice");
    db.put(2, "Bob");
}

// Große Datenbank mit optimierten Einstellungen
try (ChronMapDb<String, String> db = MapDbHelper.createLargeDb(
        "big-data",
        String.class,
        String.class,
        Serializer.STRING,
        Serializer.STRING,
        1_000_000,  // 1 Million Einträge
        50,         // 50 Bytes durchschnittliche Schlüsselgröße
        500)) {     // 500 Bytes durchschnittliche Wertgröße
    
    // Daten einfügen...
}

// Temporäre Datenbank für Tests (kein Singleton)
try (ChronMapDb<String, String> db = MapDbHelper.createTemporaryDb(
        String.class,
        String.class,
        Serializer.STRING,
        Serializer.STRING)) {
    
    // Wird automatisch beim Schließen gelöscht
}
```

**Verwaltungs-Funktionen:**

```java
// Alle registrierten Instanzen anzeigen
Set<String> names = MapDbHelper.getRegisteredInstanceNames();

// Prüfen ob eine Instanz existiert
if (MapDbHelper.existsInstance("meine-db")) {
    // ...
}

// Snapshot für alle Instanzen erstellen
MapDbHelper.snapshotAll();

// Statistiken ausgeben
System.out.println(MapDbHelper.getStatistics());

// Alle Instanzen schließen (z.B. beim Herunterfahren)
MapDbHelper.closeAll();
```

### Einfache Verwendung mit Builder (neu ab Version 1.1.0)

Alternativ können Sie auch direkt den Builder verwenden:

```java
import com.cherzog.chronmapdb.ChronMapDb;
import org.mapdb.Serializer;

// Einfachste Verwendung - alles wird automatisch erstellt
try (ChronMapDb<String, String> db = new ChronMapDb.Builder<String, String>()
        .name("meine-db")  // Name wird für MapDB-Dateiname verwendet (meine-db.db)
        .types(String.class, String.class)  // Definiert Typen für ChronicleMap
        .keySerializer(Serializer.STRING)
        .valueSerializer(Serializer.STRING)
        .build()) {
    
    // Daten hinzufügen
    db.put("schluessel1", "wert1");
    db.put("schluessel2", "wert2");
    
    // Daten abrufen
    String wert = db.get("schluessel1");
    
    // Größe abfragen
    long anzahl = db.size();
}
```

### Erweiterte Konfiguration

Sie können die automatisch erstellte ChronicleMap konfigurieren:

```java
try (ChronMapDb<String, String> db = new ChronMapDb.Builder<String, String>()
        .name("optimierte-db")
        .types(String.class, String.class)
        .entries(50000)  // Erwartete Anzahl von Einträgen
        .averageKeySize(30)  // Durchschnittliche Schlüsselgröße in Bytes
        .averageValueSize(200)  // Durchschnittliche Wertgröße in Bytes
        .snapshotIntervalSeconds(60)  // Snapshot alle 60 Sekunden
        .keySerializer(Serializer.STRING)
        .valueSerializer(Serializer.STRING)
        .build()) {
    
    // Ihre Code hier...
}
```

### Manuelle Instanzierung (für fortgeschrittene Anwendungsfälle)

Für spezielle Anforderungen können Sie ChronicleMap und MapDB weiterhin manuell erstellen:

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

// ChronMapDb mit manuell erstellter ChronicleMap
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

### Singleton-Muster mit eindeutigen Namen

Die ChronMapDb unterstützt ein Singleton-Muster basierend auf Namen. Wenn Sie eine ChronMapDb-Instanz mit einem Namen erstellen, wird diese Instanz in der Anwendung eindeutig sein:

```java
// Erste Instanz mit Namen "meine-db"
ChronMapDb<String, String> db1 = new ChronMapDb.Builder<String, String>()
    .name("meine-db")  // Eindeutiger Name
    .chronicleMap(chronicleMap)
    .mapDbFile(new File("daten.db"))
    .keySerializer(Serializer.STRING)
    .valueSerializer(Serializer.STRING)
    .build();

// Zweiter Aufruf mit dem gleichen Namen gibt die gleiche Instanz zurück
ChronMapDb<String, String> db2 = new ChronMapDb.Builder<String, String>()
    .name("meine-db")  // Gleicher Name
    .chronicleMap(chronicleMap)
    .mapDbFile(new File("daten.db"))
    .keySerializer(Serializer.STRING)
    .valueSerializer(Serializer.STRING)
    .build();

// db1 und db2 sind die gleiche Instanz
assert db1 == db2;
```

**Vorteile des Singleton-Musters:**
- **Thread-sicher**: Parallele Builder-Aufrufe mit dem gleichen Namen sind sicher
- **Ressourcen-Schonung**: Keine doppelten Instanzen für die gleiche Datenbank
- **Automatische Snapshot-Wiederherstellung**: Beim ersten Erstellen werden bestehende Snapshots geladen

**Hinweis:** Wenn Sie keinen Namen angeben, wird bei jedem `build()`-Aufruf eine neue Instanz erstellt.

## API-Dokumentation

### MapDbHelper-Methoden (Utility-Klasse)

**Erstellungs-Methoden:**

- `createSimpleStringDb(String name)` - Erstellt eine einfache String-zu-String Datenbank mit Standardeinstellungen
- `createSimpleDb(String name, Class<K>, Class<V>, Serializer<K>, Serializer<V>)` - Erstellt eine typisierte Datenbank
- `createLargeDb(...)` - Erstellt eine optimierte Datenbank für große Datenmengen (100k+ Einträge, 60s Snapshot-Intervall)
- `createFastSnapshotDb(...)` - Erstellt eine Datenbank mit schnellen Snapshots (5s Intervall für kritische Daten)
- `createTemporaryDb(...)` - Erstellt eine temporäre Datenbank ohne Singleton-Verhalten

**Verwaltungs-Methoden:**

- `getRegisteredInstanceNames()` - Gibt alle registrierten Instanz-Namen zurück
- `existsInstance(String name)` - Prüft ob eine benannte Instanz existiert
- `snapshotAll()` - Erzwingt Snapshots für alle registrierten Instanzen
- `closeAll()` - Schließt alle registrierten Instanzen
- `getTotalEntryCount()` - Gibt die Gesamtanzahl aller Einträge über alle Instanzen zurück
- `getStatistics()` - Gibt formatierte Statistiken über alle Instanzen zurück

### ChronMapDb-Methoden

- `getName()` - Gibt den Namen dieser Instanz zurück (oder null)
- `put(K key, V value)` - Fügt ein Schlüssel-Wert-Paar hinzu
- `get(K key)` - Gibt den Wert für einen Schlüssel zurück
- `remove(K key)` - Entfernt einen Schlüssel
- `size()` - Gibt die Anzahl der Einträge zurück
- `isEmpty()` - Prüft ob die Map leer ist
- `containsKey(K key)` - Prüft ob ein Schlüssel existiert
- `clear()` - Löscht alle Einträge
- `snapshot()` - Erzwingt einen sofortigen Snapshot
- `getLastWrittenKey()` - Gibt den zuletzt geschriebenen Schlüssel zurück (auch nach Restore verfügbar)
- `getChronicleMap()` - Gibt die zugrunde liegende ChronicleMap zurück
- `close()` - Schließt die Ressourcen (erstellt letzten Snapshot)

### Builder-Optionen

#### Automatische Instanzierung (empfohlen)
- `name(String)` - **Erforderlich für Auto-Instanzierung**: Eindeutiger Name, wird auch für MapDB-Dateinamen verwendet
- `types(Class<K>, Class<V>)` - **Erforderlich für Auto-Instanzierung**: Typen für ChronicleMap
- `entries(long)` - Optional: Erwartete Anzahl von Einträgen (Standard: 10000)
- `averageKeySize(int)` - Optional: Durchschnittliche Schlüsselgröße in Bytes (Standard: 20)
- `averageValueSize(int)` - Optional: Durchschnittliche Wertgröße in Bytes (Standard: 100)

#### Manuelle Instanzierung
- `chronicleMap(ChronicleMap<K, V>)` - **Optional**: Manuell erstellte ChronicleMap
- `mapDbFile(File)` - **Optional**: MapDB-Datei (wird aus name abgeleitet, wenn nicht angegeben)

#### Gemeinsame Optionen
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

### Persistierungs-Garantien

- **Snapshot-Intervall**: Snapshots erfolgen periodisch (nicht nach jeder einzelnen Änderung), sobald Änderungen vorliegen
- **Datenverlust-Fenster**: Bei einem unerwarteten Absturz können maximal die Änderungen seit dem letzten Snapshot verloren gehen (Standard: max. 30 Sekunden)
- **Performance vs. Sicherheit**: Ein kürzeres Intervall erhöht die Datensicherheit, verringert aber die Performance
- **Kontrollierter Shutdown**: Beim normalen Schließen (z.B. via `close()`) wird immer ein finaler Snapshot erstellt

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