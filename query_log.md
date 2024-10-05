```sql

    UPDATE urbanizationDB SET 
        state='Alabama', 
        lat_tract=32.3182, 
        long_tract=-86.9023, 
        population=60000, 
        adj_radiuspop_5=120.5, 
        urbanindex=CASE WHEN state='Alabama' THEN 1.0 ELSE urbanindex END 
    WHERE gisjoin='G0100010';
    
```

```sql
DELETE FROM urbanizationDB WHERE id=G0100010;
```

```sql
SELECT * FROM urbanizationDB;
```

