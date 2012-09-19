DECLARE @Database VARCHAR(255)   

DECLARE DatabaseCursor CURSOR FOR  
  	SELECT name
  	FROM sys.databases
  	WHERE name not in ( 'model','tempdb')
	AND state = 0 -- Online Databases Only
	AND source_database_id IS NULL --Not a database snapshot
	AND is_in_standby = 0  
 

OPEN DatabaseCursor  

FETCH NEXT FROM DatabaseCursor INTO @Database  
WHILE @@FETCH_STATUS = 0  
 
  BEGIN
    PRINT @Database
	SELECT ps.database_id, ps.OBJECT_ID,
 ps.index_id, b.name,
 ps.avg_fragmentation_in_percent
 FROM sys.dm_db_index_physical_stats (DB_ID(), NULL, NULL, NULL, NULL) AS ps
 INNER JOIN sys.indexes AS b ON ps.OBJECT_ID = b.OBJECT_ID
 AND ps.index_id = b.index_id
 WHERE ps.database_id = DB_ID()
 ORDER BY ps.OBJECT_ID
                   
    FETCH NEXT FROM DatabaseCursor INTO @Database  
  END
    
CLOSE DatabaseCursor   
DEALLOCATE DatabaseCursor


CREATE TABLE [dbo].[IDX_FRAG](
	[databaseName] [sysname] NOT NULL,
	[ObjectName] [sysname] NOT NULL,
	[indexName] [sysname] NULL,
	[partitionNumber] [int] NULL,
	[fragmentation] [float] NULL,
	[page_count] [bigint] NULL,
	[date] [datetime] NOT NULL,
	[fill_factor] [tinyint] NULL,
	[is_padded] [bit] NULL,
	[type_desc] [nvarchar](60) NULL
) ON [PRIMARY]


 
 -- Declare varables
        DECLARE @dbID INT, @dbName VARCHAR(128), @SQL NVARCHAR(MAX), @Used_Pages int

        -- Create a temp table to store all active databases
        CREATE TABLE #databaseList
    (
          databaseID        INT
        , databaseName      VARCHAR(128)
    );

    -- we only want non-system databases who are currenlty online
    INSERT INTO #databaseList (databaseID, databaseName)
    SELECT d.database_id, d.name FROM sys.databases d where d.[state] = 0 and d.database_id > 4


    -- Loop through all databases
        WHILE (SELECT COUNT(*) FROM #databaseList) > 0  BEGIN

                -- get a database id
                SELECT TOP 1 @dbID = databaseID, @dbName = databaseName
                FROM #databaseList;






                        SET @SQL = 'INSERT INTO dbo.IDX_FRAG (databaseName, ObjectName, indexName, partitionNumber, fragmentation, fill_factor, is_padded, type_desc, page_count, [date])
                                SELECT
                                  db.name AS databaseName
                                , obj.name AS ObjectName
                                , idx.name AS indexName
                                , ps.partition_number AS partitionNumber
                                , ps.avg_fragmentation_in_percent AS fragmentation
                                ,idx.fill_factor
                                ,idx.is_padded
                                ,idx.type_desc
                                , ps.page_count
                                , GETDATE() as [date]
                        FROM sys.databases db
                          INNER JOIN sys.dm_db_index_physical_stats ('+CAST(@dbID AS VARCHAR(10))+', NULL, NULL , NULL, N''Limited'') ps
                                  ON db.database_id = ps.database_id
                          INNER JOIN '+ @dbName+'.sys.objects obj ON obj.object_id = ps.object_id
                          INNER JOIN '+ @dbName+'.sys.indexes idx ON idx.index_id = ps.index_id AND idx.object_id = ps.object_id
                        WHERE ps.index_id > 0 
                           AND ps.page_count > 100 
                           AND ps.avg_fragmentation_in_percent > 39 
                        ORDER BY page_count desc
                        OPTION (MaxDop 1);'
                      
                EXECUTE sp_executesql @SQL
                -- remove the database from the databases table
               DELETE FROM #databaseList WHERE databaseID = @dbID

                -- get the next database in the databases table
                SELECT TOP 1 @dbID = databaseID, @dbName = databaseName
                FROM #databaseList;

        END
        -- temp table is no longer needed, so we will kill it.
        
        
        DROP TABLE #databaseList
        --drop table dbo.IDX_FRAG
        
        --select * from sys.allocation_units
        --select * from sys.dm_db_index_physical_stats ('15', NULL, NULL , NULL, N'Limited')
        
        --select * from sys.indexes
        
        
      
 
 EXEC sp_msforeachdb 'select "?" AS db, * from [?].sys.allocation_units'