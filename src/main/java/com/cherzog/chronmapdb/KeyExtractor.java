package com.cherzog.chronmapdb;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Interface for extracting keys from various sources.
 * Implementations must ensure high performance as millions of records may be processed.
 *
 * @param <K> The type of the key
 */
@FunctionalInterface
public interface KeyExtractor<K> {
    
    /**
     * Extracts a key from the given source.
     *
     * @param source The source object from which to extract the key
     * @return The extracted key
     * @throws IllegalArgumentException if the source is invalid
     */
    K extractKey(Object source);
    
    /**
     * Creates a KeyExtractor that returns the source object as-is.
     * Useful for simple String, Timestamp, or UUID keys.
     *
     * @param <K> The type of the key
     * @return A KeyExtractor that performs no transformation
     */
    @SuppressWarnings("unchecked")
    static <K> KeyExtractor<K> identity() {
        return source -> (K) source;
    }
    
    /**
     * Creates a KeyExtractor that concatenates array values with '\0' separator.
     * Uses StringBuilder for maximum performance.
     *
     * @return A KeyExtractor for arrays
     */
    static KeyExtractor<String> fromArray() {
        return source -> {
            if (source == null) {
                throw new IllegalArgumentException("Key source cannot be null");
            }
            
            if (!source.getClass().isArray()) {
                throw new IllegalArgumentException("Expected array but got " + source.getClass().getName());
            }
            
            Object[] array = (Object[]) source;
            if (array.length == 0) {
                throw new IllegalArgumentException("Key array cannot be empty");
            }
            
            // Optimize for single element
            if (array.length == 1) {
                return array[0] == null ? "" : array[0].toString();
            }
            
            // Use StringBuilder for performance with multiple elements
            StringBuilder sb = new StringBuilder();
            sb.append(array[0] == null ? "" : array[0].toString());
            
            for (int i = 1; i < array.length; i++) {
                sb.append('\0');
                sb.append(array[i] == null ? "" : array[i].toString());
            }
            
            return sb.toString();
        };
    }
    
    /**
     * Creates a KeyExtractor that extracts values from a ResultSet by column indices.
     * If a single index is provided, returns the value directly.
     * If multiple indices are provided, concatenates values with '\0' separator.
     *
     * @param columnIndices The 1-based column indices to extract (SQL convention)
     * @return A KeyExtractor for ResultSet by column indices
     * @throws IllegalArgumentException if no column indices are provided
     */
    static KeyExtractor<String> fromResultSetByIndex(int... columnIndices) {
        if (columnIndices == null || columnIndices.length == 0) {
            throw new IllegalArgumentException("At least one column index must be provided");
        }
        
        // Validate indices
        for (int index : columnIndices) {
            if (index < 1) {
                throw new IllegalArgumentException("Column indices must be >= 1 (SQL convention)");
            }
        }
        
        return source -> {
            if (!(source instanceof ResultSet)) {
                throw new IllegalArgumentException("Expected ResultSet but got " + source.getClass().getName());
            }
            
            ResultSet rs = (ResultSet) source;
            
            try {
                // Optimize for single column
                if (columnIndices.length == 1) {
                    Object value = rs.getObject(columnIndices[0]);
                    return value == null ? "" : value.toString();
                }
                
                // Multiple columns - use StringBuilder
                StringBuilder sb = new StringBuilder();
                Object value = rs.getObject(columnIndices[0]);
                sb.append(value == null ? "" : value.toString());
                
                for (int i = 1; i < columnIndices.length; i++) {
                    sb.append('\0');
                    value = rs.getObject(columnIndices[i]);
                    sb.append(value == null ? "" : value.toString());
                }
                
                return sb.toString();
            } catch (SQLException e) {
                throw new IllegalArgumentException("Failed to extract key from ResultSet: " + e.getMessage(), e);
            }
        };
    }
    
    /**
     * Creates a KeyExtractor that extracts values from a ResultSet by column names.
     * If a single name is provided, returns the value directly.
     * If multiple names are provided, concatenates values with '\0' separator.
     *
     * @param columnNames The column names to extract
     * @return A KeyExtractor for ResultSet by column names
     * @throws IllegalArgumentException if no column names are provided
     */
    static KeyExtractor<String> fromResultSetByName(String... columnNames) {
        if (columnNames == null || columnNames.length == 0) {
            throw new IllegalArgumentException("At least one column name must be provided");
        }
        
        // Validate column names
        for (String name : columnNames) {
            if (name == null || name.trim().isEmpty()) {
                throw new IllegalArgumentException("Column names cannot be null or empty");
            }
        }
        
        return source -> {
            if (!(source instanceof ResultSet)) {
                throw new IllegalArgumentException("Expected ResultSet but got " + source.getClass().getName());
            }
            
            ResultSet rs = (ResultSet) source;
            
            try {
                // Optimize for single column
                if (columnNames.length == 1) {
                    Object value = rs.getObject(columnNames[0]);
                    return value == null ? "" : value.toString();
                }
                
                // Multiple columns - use StringBuilder
                StringBuilder sb = new StringBuilder();
                Object value = rs.getObject(columnNames[0]);
                sb.append(value == null ? "" : value.toString());
                
                for (int i = 1; i < columnNames.length; i++) {
                    sb.append('\0');
                    value = rs.getObject(columnNames[i]);
                    sb.append(value == null ? "" : value.toString());
                }
                
                return sb.toString();
            } catch (SQLException e) {
                throw new IllegalArgumentException("Failed to extract key from ResultSet: " + e.getMessage(), e);
            }
        };
    }
}
