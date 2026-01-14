package com.cherzog.chronmapdb;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests für die KeyExtractor-Funktionalität.
 */
class KeyExtractorTest {
    
    @Test
    void testIdentityExtractor() {
        KeyExtractor<String> extractor = KeyExtractor.identity();
        
        String key = "testKey";
        assertEquals(key, extractor.extractKey(key));
        
        Integer intKey = 123;
        KeyExtractor<Integer> intExtractor = KeyExtractor.identity();
        assertEquals(intKey, intExtractor.extractKey(intKey));
    }
    
    @Test
    void testArrayExtractorSingleElement() {
        KeyExtractor<String> extractor = KeyExtractor.fromArray();
        
        String[] array = {"value1"};
        assertEquals("value1", extractor.extractKey(array));
    }
    
    @Test
    void testArrayExtractorMultipleElements() {
        KeyExtractor<String> extractor = KeyExtractor.fromArray();
        
        String[] array = {"value1", "value2", "value3"};
        assertEquals("value1\0value2\0value3", extractor.extractKey(array));
    }
    
    @Test
    void testArrayExtractorWithNullValues() {
        KeyExtractor<String> extractor = KeyExtractor.fromArray();
        
        String[] array = {"value1", null, "value3"};
        assertEquals("value1\0\0value3", extractor.extractKey(array));
    }
    
    @Test
    void testArrayExtractorRejectsNullArray() {
        KeyExtractor<String> extractor = KeyExtractor.fromArray();
        
        assertThrows(IllegalArgumentException.class, () -> extractor.extractKey(null));
    }
    
    @Test
    void testArrayExtractorRejectsEmptyArray() {
        KeyExtractor<String> extractor = KeyExtractor.fromArray();
        
        String[] emptyArray = {};
        assertThrows(IllegalArgumentException.class, () -> extractor.extractKey(emptyArray));
    }
    
    @Test
    void testArrayExtractorRejectsNonArray() {
        KeyExtractor<String> extractor = KeyExtractor.fromArray();
        
        assertThrows(IllegalArgumentException.class, () -> extractor.extractKey("not an array"));
    }
    
    @Test
    void testArrayExtractorWithIntegerArray() {
        KeyExtractor<String> extractor = KeyExtractor.fromArray();
        
        Integer[] array = {1, 2, 3};
        assertEquals("1\02\03", extractor.extractKey(array));
    }
    
    @Test
    void testResultSetByIndexSingleColumn() throws SQLException {
        KeyExtractor<String> extractor = KeyExtractor.fromResultSetByIndex(1);
        
        ResultSet rs = Mockito.mock(ResultSet.class);
        Mockito.when(rs.getObject(1)).thenReturn("value1");
        
        assertEquals("value1", extractor.extractKey(rs));
    }
    
    @Test
    void testResultSetByIndexMultipleColumns() throws SQLException {
        KeyExtractor<String> extractor = KeyExtractor.fromResultSetByIndex(1, 2, 3);
        
        ResultSet rs = Mockito.mock(ResultSet.class);
        Mockito.when(rs.getObject(1)).thenReturn("value1");
        Mockito.when(rs.getObject(2)).thenReturn(123);
        Mockito.when(rs.getObject(3)).thenReturn("value3");
        
        assertEquals("value1\0123\0value3", extractor.extractKey(rs));
    }
    
    @Test
    void testResultSetByIndexWithNullValues() throws SQLException {
        KeyExtractor<String> extractor = KeyExtractor.fromResultSetByIndex(1, 2);
        
        ResultSet rs = Mockito.mock(ResultSet.class);
        Mockito.when(rs.getObject(1)).thenReturn("value1");
        Mockito.when(rs.getObject(2)).thenReturn(null);
        
        assertEquals("value1\0", extractor.extractKey(rs));
    }
    
    @Test
    void testResultSetByIndexRejectsInvalidIndex() {
        assertThrows(IllegalArgumentException.class, () -> 
            KeyExtractor.fromResultSetByIndex(0));
        
        assertThrows(IllegalArgumentException.class, () -> 
            KeyExtractor.fromResultSetByIndex(-1));
    }
    
    @Test
    void testResultSetByIndexRejectsEmptyIndices() {
        assertThrows(IllegalArgumentException.class, () -> 
            KeyExtractor.fromResultSetByIndex());
    }
    
    @Test
    void testResultSetByIndexRejectsNonResultSet() {
        KeyExtractor<String> extractor = KeyExtractor.fromResultSetByIndex(1);
        
        assertThrows(IllegalArgumentException.class, () -> 
            extractor.extractKey("not a ResultSet"));
    }
    
    @Test
    void testResultSetByIndexHandlesSQLException() throws SQLException {
        KeyExtractor<String> extractor = KeyExtractor.fromResultSetByIndex(1);
        
        ResultSet rs = Mockito.mock(ResultSet.class);
        Mockito.when(rs.getObject(1)).thenThrow(new SQLException("Test exception"));
        
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> 
            extractor.extractKey(rs));
        assertTrue(exception.getMessage().contains("Failed to extract key from ResultSet"));
    }
    
    @Test
    void testResultSetByNameSingleColumn() throws SQLException {
        KeyExtractor<String> extractor = KeyExtractor.fromResultSetByName("ID");
        
        ResultSet rs = Mockito.mock(ResultSet.class);
        Mockito.when(rs.getObject("ID")).thenReturn("value1");
        
        assertEquals("value1", extractor.extractKey(rs));
    }
    
    @Test
    void testResultSetByNameMultipleColumns() throws SQLException {
        KeyExtractor<String> extractor = KeyExtractor.fromResultSetByName("ID", "NAME", "VALUE");
        
        ResultSet rs = Mockito.mock(ResultSet.class);
        Mockito.when(rs.getObject("ID")).thenReturn(123);
        Mockito.when(rs.getObject("NAME")).thenReturn("John");
        Mockito.when(rs.getObject("VALUE")).thenReturn(45.67);
        
        assertEquals("123\0John\045.67", extractor.extractKey(rs));
    }
    
    @Test
    void testResultSetByNameWithNullValues() throws SQLException {
        KeyExtractor<String> extractor = KeyExtractor.fromResultSetByName("ID", "NAME");
        
        ResultSet rs = Mockito.mock(ResultSet.class);
        Mockito.when(rs.getObject("ID")).thenReturn(123);
        Mockito.when(rs.getObject("NAME")).thenReturn(null);
        
        assertEquals("123\0", extractor.extractKey(rs));
    }
    
    @Test
    void testResultSetByNameRejectsNullName() {
        assertThrows(IllegalArgumentException.class, () -> 
            KeyExtractor.fromResultSetByName((String) null));
    }
    
    @Test
    void testResultSetByNameRejectsEmptyName() {
        assertThrows(IllegalArgumentException.class, () -> 
            KeyExtractor.fromResultSetByName(""));
        
        assertThrows(IllegalArgumentException.class, () -> 
            KeyExtractor.fromResultSetByName("   "));
    }
    
    @Test
    void testResultSetByNameRejectsEmptyNames() {
        assertThrows(IllegalArgumentException.class, () -> 
            KeyExtractor.fromResultSetByName());
    }
    
    @Test
    void testResultSetByNameRejectsNonResultSet() {
        KeyExtractor<String> extractor = KeyExtractor.fromResultSetByName("ID");
        
        assertThrows(IllegalArgumentException.class, () -> 
            extractor.extractKey("not a ResultSet"));
    }
    
    @Test
    void testResultSetByNameHandlesSQLException() throws SQLException {
        KeyExtractor<String> extractor = KeyExtractor.fromResultSetByName("ID");
        
        ResultSet rs = Mockito.mock(ResultSet.class);
        Mockito.when(rs.getObject("ID")).thenThrow(new SQLException("Test exception"));
        
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> 
            extractor.extractKey(rs));
        assertTrue(exception.getMessage().contains("Failed to extract key from ResultSet"));
    }
    
    @Test
    void testPerformanceArrayConcatenation() {
        KeyExtractor<String> extractor = KeyExtractor.fromArray();
        String[] array = {"value1", "value2", "value3", "value4", "value5"};
        
        long startTime = System.nanoTime();
        for (int i = 0; i < 100000; i++) {
            extractor.extractKey(array);
        }
        long endTime = System.nanoTime();
        
        long durationMs = (endTime - startTime) / 1_000_000;
        // Sollte weniger als 1 Sekunde für 100.000 Operationen dauern
        assertTrue(durationMs < 1000, "Array concatenation took " + durationMs + "ms for 100,000 operations");
    }
}
