--Requires VIEW SERVER STATE permission on the server 
--Replace all [TotalAgility] with name of your TotalAgility database eg [TotalAgility] with [TotalAgility_UAT]. For OPMT replace all [TotalAgility] with the name of your tenant main database eg [TotalAgility_Tenant]
--Replace all [TotalAgility_Documents] with name of your TotalAgility Documents database eg [TotalAgility_Documents] with [TotalAgility_Documents_UAT].   For OPMT replace all [TotalAgility_Documents] with the name of your tenant Documents database eg [TotalAgility_Tenant]
--Replace all [TotalAgility_FinishedJobs] with name of your TotalAgility Finished jobs database eg [TotalAgility_FinishedJobs] with [TotalAgility_FinishedJobs_UAT]. For OPMT replace all [TotalAgility_FinishedJobs] with the name of your tenant FinishedJobs database eg [TotalAgility_Tenant]
--Replace all [TotalAgility_Reporting] with name of your TotalAgility Reporting  database eg [TotalAgility_Reporting] with [TotalAgility_Reporting_UAT]. For OPMT replace all [TotalAgility_Reporting] with the name of your tenant reporting database eg [TotalAgility_Tenant]
--Replace all [TotalAgility_Reporting_Staging] with name of your TotalAgility Reporting  database eg [TotalAgility_Reporting_Staging] with [TotalAgility_Reporting_Staging_UAT]. For OPMT replace all [TotalAgility_Reporting_Staging] with the name of your tenant reporting database eg [TotalAgility_Tenant]
--For OPMT, replace all [dbo] with [live] or [dev] as per the OPMT deployment type.
--Then right click on the query window, Results To, and select "Results to file" 
--Once the queries complete, please attach the results RPT file to your Kofax Support case  
SET DEADLOCK_PRIORITY LOW;  
GO  
SET NOCOUNT ON
RAISERROR('Basic SQL version and edition info', 0, 1) WITH NOWAIT
RAISERROR('Progress 1pc completed', 0, 1) WITH NOWAIT
SELECT @@VERSION AS Version, ServerProperty('ServerName') AS ServerName, ServerProperty('IsClustered') AS IsClustered, ServerProperty('IsHadrEnabled') AS AlwaysOnAGEnabled
SELECT d.*, dm.encryption_state
FROM sys.databases AS d  
LEFT JOIN sys.dm_database_encryption_keys dm ON d.database_id = dm.database_id

RAISERROR('Progress 2pc completed', 0, 1) WITH NOWAIT

RAISERROR('Check if any trace flags are enabled (notably 9481 enables LCE prior to new option in SQL Server 2016) (Requires membership in the public role)', 0, 1) WITH NOWAIT
DBCC TRACESTATUS(-1)

RAISERROR('Progress 3pc completed', 0, 1) WITH NOWAIT 
RAISERROR('Database file sizes and permissions', 0, 1) WITH NOWAIT
SELECT DB_NAME(database_id) AS DatabaseName,
Name AS Logical_Name,
Physical_Name, (size*8)/1024 SizeMB
FROM sys.master_files
ORDER BY size DESC

RAISERROR('Progress 4pc completed', 0, 1) WITH NOWAIT
RAISERROR('Viewing Aggregate disk I/O latency Information - On well-configured storage which isn’t being overloaded I’d expect to see single-digit ms for either read or write latency.  Beware: This information is the aggregate of all I/Os performed since the database was brought online, so the I/O times reported by the script are averages. https://www.sqlskills.com/blogs/paul/how-to-examine-io-subsystem-latencies-from-within-sql-server/', 0, 1) WITH NOWAIT
SELECT
    [ReadLatency] =
        CASE WHEN [num_of_reads] = 0
            THEN 0 ELSE ([io_stall_read_ms] / [num_of_reads]) END,
    [WriteLatency] =
        CASE WHEN [num_of_writes] = 0
            THEN 0 ELSE ([io_stall_write_ms] / [num_of_writes]) END,
    [Latency] =
        CASE WHEN ([num_of_reads] = 0 AND [num_of_writes] = 0)
            THEN 0 ELSE ([io_stall] / ([num_of_reads] + [num_of_writes])) END,
    [AvgBPerRead] =
        CASE WHEN [num_of_reads] = 0
            THEN 0 ELSE ([num_of_bytes_read] / [num_of_reads]) END,
    [AvgBPerWrite] =
        CASE WHEN [num_of_writes] = 0
            THEN 0 ELSE ([num_of_bytes_written] / [num_of_writes]) END,
    [AvgBPerTransfer] =
        CASE WHEN ([num_of_reads] = 0 AND [num_of_writes] = 0)
            THEN 0 ELSE
                (([num_of_bytes_read] + [num_of_bytes_written]) /
                ([num_of_reads] + [num_of_writes])) END,
    LEFT ([mf].[physical_name], 2) AS [Drive],
    DB_NAME ([vfs].[database_id]) AS [DB],
    [mf].[physical_name]
FROM
    sys.dm_io_virtual_file_stats (NULL,NULL) AS [vfs]
JOIN sys.master_files AS [mf]
    ON [vfs].[database_id] = [mf].[database_id]
    AND [vfs].[file_id] = [mf].[file_id]
-- WHERE [vfs].[file_id] = 2 -- log files
-- ORDER BY [Latency] DESC
--ORDER BY [ReadLatency] DESC
ORDER BY [WriteLatency] DESC;
GO


RAISERROR('This query shows a snapshot of 10 seconds of wait types.  If you see high waits types from this query (latch) and high aggregate read/write values from the above query, its a sign of sub system contention.  It could mean read and write hot spots and then drill into these database to see what’s going on and if its expected. If the data is fine, it could mean sub system contention - if so, the SAN admin may need to move those hot spot files to dedicated and/or faster storage      ', 0, 1) WITH NOWAIT
IF EXISTS (SELECT * FROM [tempdb].[sys].[objects]
    WHERE [name] = N'##SQLStats1')
    DROP TABLE [##SQLStats1];
  
IF EXISTS (SELECT * FROM [tempdb].[sys].[objects]
    WHERE [name] = N'##SQLStats2')
    DROP TABLE [##SQLStats2];
GO
  
SELECT [wait_type], [waiting_tasks_count], [wait_time_ms],
       [max_wait_time_ms], [signal_wait_time_ms]
INTO ##SQLStats1
FROM sys.dm_os_wait_stats;
GO
  
WAITFOR DELAY '00:00:10';
GO
  
SELECT [wait_type], [waiting_tasks_count], [wait_time_ms],
       [max_wait_time_ms], [signal_wait_time_ms]
INTO ##SQLStats2
FROM sys.dm_os_wait_stats;
GO
  
WITH [DiffWaits] AS
(SELECT
-- Waits that weren't in the first snapshot
        [ts2].[wait_type],
        [ts2].[wait_time_ms],
        [ts2].[signal_wait_time_ms],
        [ts2].[waiting_tasks_count]
    FROM [##SQLStats2] AS [ts2]
    LEFT OUTER JOIN [##SQLStats1] AS [ts1]
        ON [ts2].[wait_type] = [ts1].[wait_type]
    WHERE [ts1].[wait_type] IS NULL
    AND [ts2].[wait_time_ms] > 0
UNION
SELECT
-- Diff of waits in both snapshots
        [ts2].[wait_type],
        [ts2].[wait_time_ms] - [ts1].[wait_time_ms] AS [wait_time_ms],
        [ts2].[signal_wait_time_ms] - [ts1].[signal_wait_time_ms] AS [signal_wait_time_ms],
        [ts2].[waiting_tasks_count] - [ts1].[waiting_tasks_count] AS [waiting_tasks_count]
    FROM [##SQLStats2] AS [ts2]
    LEFT OUTER JOIN [##SQLStats1] AS [ts1]
        ON [ts2].[wait_type] = [ts1].[wait_type]
    WHERE [ts1].[wait_type] IS NOT NULL
    AND [ts2].[waiting_tasks_count] - [ts1].[waiting_tasks_count] > 0
    AND [ts2].[wait_time_ms] - [ts1].[wait_time_ms] > 0),
[Waits] AS
    (SELECT
        [wait_type],
        [wait_time_ms] / 1000.0 AS [WaitS],
        ([wait_time_ms] - [signal_wait_time_ms]) / 1000.0 AS [ResourceS],
        [signal_wait_time_ms] / 1000.0 AS [SignalS],
        [waiting_tasks_count] AS [WaitCount],
        100.0 * [wait_time_ms] / SUM ([wait_time_ms]) OVER() AS [Percentage],
        ROW_NUMBER() OVER(ORDER BY [wait_time_ms] DESC) AS [RowNum]
    FROM [DiffWaits]
    WHERE [wait_type] NOT IN (
        -- These wait types are almost 100% never a problem and so they are
        -- filtered out to avoid them skewing the results. Click on the URL
        -- for more information.
        N'BROKER_EVENTHANDLER', -- https://www.sqlskills.com/help/waits/BROKER_EVENTHANDLER
        N'BROKER_RECEIVE_WAITFOR', -- https://www.sqlskills.com/help/waits/BROKER_RECEIVE_WAITFOR
        N'BROKER_TASK_STOP', -- https://www.sqlskills.com/help/waits/BROKER_TASK_STOP
        N'BROKER_TO_FLUSH', -- https://www.sqlskills.com/help/waits/BROKER_TO_FLUSH
        N'BROKER_TRANSMITTER', -- https://www.sqlskills.com/help/waits/BROKER_TRANSMITTER
        N'CHECKPOINT_QUEUE', -- https://www.sqlskills.com/help/waits/CHECKPOINT_QUEUE
        N'CHKPT', -- https://www.sqlskills.com/help/waits/CHKPT
        N'CLR_AUTO_EVENT', -- https://www.sqlskills.com/help/waits/CLR_AUTO_EVENT
        N'CLR_MANUAL_EVENT', -- https://www.sqlskills.com/help/waits/CLR_MANUAL_EVENT
        N'CLR_SEMAPHORE', -- https://www.sqlskills.com/help/waits/CLR_SEMAPHORE
        N'CXCONSUMER', -- https://www.sqlskills.com/help/waits/CXCONSUMER
 
        -- Maybe comment these four out if you have mirroring issues
        N'DBMIRROR_DBM_EVENT', -- https://www.sqlskills.com/help/waits/DBMIRROR_DBM_EVENT
        N'DBMIRROR_EVENTS_QUEUE', -- https://www.sqlskills.com/help/waits/DBMIRROR_EVENTS_QUEUE
        N'DBMIRROR_WORKER_QUEUE', -- https://www.sqlskills.com/help/waits/DBMIRROR_WORKER_QUEUE
        N'DBMIRRORING_CMD', -- https://www.sqlskills.com/help/waits/DBMIRRORING_CMD
 
        N'DIRTY_PAGE_POLL', -- https://www.sqlskills.com/help/waits/DIRTY_PAGE_POLL
        N'DISPATCHER_QUEUE_SEMAPHORE', -- https://www.sqlskills.com/help/waits/DISPATCHER_QUEUE_SEMAPHORE
        N'EXECSYNC', -- https://www.sqlskills.com/help/waits/EXECSYNC
        N'FSAGENT', -- https://www.sqlskills.com/help/waits/FSAGENT
        N'FT_IFTS_SCHEDULER_IDLE_WAIT', -- https://www.sqlskills.com/help/waits/FT_IFTS_SCHEDULER_IDLE_WAIT
        N'FT_IFTSHC_MUTEX', -- https://www.sqlskills.com/help/waits/FT_IFTSHC_MUTEX
 
        -- Maybe comment these six out if you have AG issues
        N'HADR_CLUSAPI_CALL', -- https://www.sqlskills.com/help/waits/HADR_CLUSAPI_CALL
        N'HADR_FILESTREAM_IOMGR_IOCOMPLETION', -- https://www.sqlskills.com/help/waits/HADR_FILESTREAM_IOMGR_IOCOMPLETION
        N'HADR_LOGCAPTURE_WAIT', -- https://www.sqlskills.com/help/waits/HADR_LOGCAPTURE_WAIT
        N'HADR_NOTIFICATION_DEQUEUE', -- https://www.sqlskills.com/help/waits/HADR_NOTIFICATION_DEQUEUE
        N'HADR_TIMER_TASK', -- https://www.sqlskills.com/help/waits/HADR_TIMER_TASK
        N'HADR_WORK_QUEUE', -- https://www.sqlskills.com/help/waits/HADR_WORK_QUEUE
 
        N'KSOURCE_WAKEUP', -- https://www.sqlskills.com/help/waits/KSOURCE_WAKEUP
        N'LAZYWRITER_SLEEP', -- https://www.sqlskills.com/help/waits/LAZYWRITER_SLEEP
        N'LOGMGR_QUEUE', -- https://www.sqlskills.com/help/waits/LOGMGR_QUEUE
        N'MEMORY_ALLOCATION_EXT', -- https://www.sqlskills.com/help/waits/MEMORY_ALLOCATION_EXT
        N'ONDEMAND_TASK_QUEUE', -- https://www.sqlskills.com/help/waits/ONDEMAND_TASK_QUEUE
        N'PARALLEL_REDO_DRAIN_WORKER', -- https://www.sqlskills.com/help/waits/PARALLEL_REDO_DRAIN_WORKER
        N'PARALLEL_REDO_LOG_CACHE', -- https://www.sqlskills.com/help/waits/PARALLEL_REDO_LOG_CACHE
        N'PARALLEL_REDO_TRAN_LIST', -- https://www.sqlskills.com/help/waits/PARALLEL_REDO_TRAN_LIST
        N'PARALLEL_REDO_WORKER_SYNC', -- https://www.sqlskills.com/help/waits/PARALLEL_REDO_WORKER_SYNC
        N'PARALLEL_REDO_WORKER_WAIT_WORK', -- https://www.sqlskills.com/help/waits/PARALLEL_REDO_WORKER_WAIT_WORK
        N'PREEMPTIVE_XE_GETTARGETSTATE', -- https://www.sqlskills.com/help/waits/PREEMPTIVE_XE_GETTARGETSTATE
        N'PWAIT_ALL_COMPONENTS_INITIALIZED', -- https://www.sqlskills.com/help/waits/PWAIT_ALL_COMPONENTS_INITIALIZED
        N'PWAIT_DIRECTLOGCONSUMER_GETNEXT', -- https://www.sqlskills.com/help/waits/PWAIT_DIRECTLOGCONSUMER_GETNEXT
        N'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP', -- https://www.sqlskills.com/help/waits/QDS_PERSIST_TASK_MAIN_LOOP_SLEEP
        N'QDS_ASYNC_QUEUE', -- https://www.sqlskills.com/help/waits/QDS_ASYNC_QUEUE
        N'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP',
            -- https://www.sqlskills.com/help/waits/QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP
        N'QDS_SHUTDOWN_QUEUE', -- https://www.sqlskills.com/help/waits/QDS_SHUTDOWN_QUEUE
        N'REDO_THREAD_PENDING_WORK', -- https://www.sqlskills.com/help/waits/REDO_THREAD_PENDING_WORK
        N'REQUEST_FOR_DEADLOCK_SEARCH', -- https://www.sqlskills.com/help/waits/REQUEST_FOR_DEADLOCK_SEARCH
        N'RESOURCE_QUEUE', -- https://www.sqlskills.com/help/waits/RESOURCE_QUEUE
        N'SERVER_IDLE_CHECK', -- https://www.sqlskills.com/help/waits/SERVER_IDLE_CHECK
        N'SLEEP_BPOOL_FLUSH', -- https://www.sqlskills.com/help/waits/SLEEP_BPOOL_FLUSH
        N'SLEEP_DBSTARTUP', -- https://www.sqlskills.com/help/waits/SLEEP_DBSTARTUP
        N'SLEEP_DCOMSTARTUP', -- https://www.sqlskills.com/help/waits/SLEEP_DCOMSTARTUP
        N'SLEEP_MASTERDBREADY', -- https://www.sqlskills.com/help/waits/SLEEP_MASTERDBREADY
        N'SLEEP_MASTERMDREADY', -- https://www.sqlskills.com/help/waits/SLEEP_MASTERMDREADY
        N'SLEEP_MASTERUPGRADED', -- https://www.sqlskills.com/help/waits/SLEEP_MASTERUPGRADED
        N'SLEEP_MSDBSTARTUP', -- https://www.sqlskills.com/help/waits/SLEEP_MSDBSTARTUP
        N'SLEEP_SYSTEMTASK', -- https://www.sqlskills.com/help/waits/SLEEP_SYSTEMTASK
        N'SLEEP_TASK', -- https://www.sqlskills.com/help/waits/SLEEP_TASK
        N'SLEEP_TEMPDBSTARTUP', -- https://www.sqlskills.com/help/waits/SLEEP_TEMPDBSTARTUP
        N'SNI_HTTP_ACCEPT', -- https://www.sqlskills.com/help/waits/SNI_HTTP_ACCEPT
        N'SOS_WORK_DISPATCHER', -- https://www.sqlskills.com/help/waits/SOS_WORK_DISPATCHER
        N'SP_SERVER_DIAGNOSTICS_SLEEP', -- https://www.sqlskills.com/help/waits/SP_SERVER_DIAGNOSTICS_SLEEP
        N'SQLTRACE_BUFFER_FLUSH', -- https://www.sqlskills.com/help/waits/SQLTRACE_BUFFER_FLUSH
        N'SQLTRACE_INCREMENTAL_FLUSH_SLEEP', -- https://www.sqlskills.com/help/waits/SQLTRACE_INCREMENTAL_FLUSH_SLEEP
        N'SQLTRACE_WAIT_ENTRIES', -- https://www.sqlskills.com/help/waits/SQLTRACE_WAIT_ENTRIES
        N'WAIT_FOR_RESULTS', -- https://www.sqlskills.com/help/waits/WAIT_FOR_RESULTS
        N'WAITFOR', -- https://www.sqlskills.com/help/waits/WAITFOR
        N'WAITFOR_TASKSHUTDOWN', -- https://www.sqlskills.com/help/waits/WAITFOR_TASKSHUTDOWN
        N'WAIT_XTP_RECOVERY', -- https://www.sqlskills.com/help/waits/WAIT_XTP_RECOVERY
        N'WAIT_XTP_HOST_WAIT', -- https://www.sqlskills.com/help/waits/WAIT_XTP_HOST_WAIT
        N'WAIT_XTP_OFFLINE_CKPT_NEW_LOG', -- https://www.sqlskills.com/help/waits/WAIT_XTP_OFFLINE_CKPT_NEW_LOG
        N'WAIT_XTP_CKPT_CLOSE', -- https://www.sqlskills.com/help/waits/WAIT_XTP_CKPT_CLOSE
        N'XE_DISPATCHER_JOIN', -- https://www.sqlskills.com/help/waits/XE_DISPATCHER_JOIN
        N'XE_DISPATCHER_WAIT', -- https://www.sqlskills.com/help/waits/XE_DISPATCHER_WAIT
        N'XE_TIMER_EVENT' -- https://www.sqlskills.com/help/waits/XE_TIMER_EVENT
    )
    )
SELECT
    [W1].[wait_type] AS [WaitType],
    CAST ([W1].[WaitS] AS DECIMAL (16, 2)) AS [Wait_Sec],
    CAST ([W1].[ResourceS] AS DECIMAL (16, 2)) AS [Resource_Sec],
    CAST ([W1].[SignalS] AS DECIMAL (16, 2)) AS [Signal_Sec],
    [W1].[WaitCount] AS [WaitCount],
    CAST ([W1].[Percentage] AS DECIMAL (5, 2)) AS [Percentage],
    CAST (([W1].[WaitS] / [W1].[WaitCount]) AS DECIMAL (16, 4)) AS [AvgWait_Sec],
    CAST (([W1].[ResourceS] / [W1].[WaitCount]) AS DECIMAL (16, 4)) AS [AvgRes_Sec],
    CAST (([W1].[SignalS] / [W1].[WaitCount]) AS DECIMAL (16, 4)) AS [AvgSig_Sec]
FROM [Waits] AS [W1]
INNER JOIN [Waits] AS [W2]
    ON [W2].[RowNum] <= [W1].[RowNum]
GROUP BY [W1].[RowNum], [W1].[wait_type], [W1].[WaitS],
    [W1].[ResourceS], [W1].[SignalS], [W1].[WaitCount], [W1].[Percentage]
HAVING SUM ([W2].[Percentage]) - [W1].[Percentage] < 95; -- percentage threshold
GO
  
-- Cleanup
IF EXISTS (SELECT * FROM [tempdb].[sys].[objects]
    WHERE [name] = N'##SQLStats1')
    DROP TABLE [##SQLStats1];
  
IF EXISTS (SELECT * FROM [tempdb].[sys].[objects]
    WHERE [name] = N'##SQLStats2')
    DROP TABLE [##SQLStats2];
GO

RAISERROR('Progress 5pc completed', 0, 1) WITH NOWAIT
RAISERROR('Checks size of platform databases.  The preference is to have well maintained databases, however, it is common for customers to have huge databases', 0, 1) WITH NOWAIT
use [TotalAgility]
EXEC  sp_msforeachtable 'EXEC sp_spaceused [?]' 
RAISERROR('Progress 6pc completed', 0, 1) WITH NOWAIT
use [TotalAgility_Documents]
EXEC  sp_msforeachtable 'EXEC sp_spaceused [?]' 
RAISERROR('Progress 7pc completed', 0, 1) WITH NOWAIT
use [TotalAgility_FinishedJobs]
EXEC  sp_msforeachtable 'EXEC sp_spaceused [?]' 
RAISERROR('Progress 8pc completed', 0, 1) WITH NOWAIT
use [TotalAgility_Reporting]
EXEC  sp_msforeachtable 'EXEC sp_spaceused [?]' 
RAISERROR('Progress 9pc completed', 0, 1) WITH NOWAIT
use [TotalAgility_Reporting_Staging]
EXEC  sp_msforeachtable 'EXEC sp_spaceused [?]' 
RAISERROR('Progress 10pc completed', 0, 1) WITH NOWAIT

RAISERROR('Who is connected from where using which authentication ', 0, 1) WITH NOWAIT
SELECT  S.login_name, C.auth_scheme, s.host_name, COUNT(*) AS Count
FROM sys.dm_exec_connections AS C
JOIN sys.dm_exec_sessions AS S ON C.session_id = S.session_id
GROUP BY S.login_name, C.auth_scheme, s.host_name
ORDER BY COUNT(*) DESC


-- 
RAISERROR('Gathering Database file(data/log) info - if the customer has log file issues, this is useful to know auto growth settings.  ', 0, 1) WITH NOWAIT
-- Drop temporary table if it exists
IF OBJECT_ID('tempdb..#info') IS NOT NULL
       DROP TABLE #info;
 
-- Create table to store database file information
CREATE TABLE #DBAgentFileinfo (
     databasename VARCHAR(128)
     ,name VARCHAR(128)
    ,fileid INT
    ,filename VARCHAR(1000)
    ,filegroup VARCHAR(128)
    ,size VARCHAR(25)
    ,maxsize VARCHAR(25)
    ,growth VARCHAR(25)
    ,usage VARCHAR(25));
    
-- Get database file information for each database   
SET NOCOUNT ON; 
INSERT INTO #DBAgentFileinfo
EXEC sp_MSforeachdb ' 
select ''?'',name,  fileid, filename,
filegroup = filegroup_name(groupid),
''size'' = convert(nvarchar(15), convert (bigint, size) * 8) + N'' KB'',
''maxsize'' = (case maxsize when -1 then N''Unlimited''
else
convert(nvarchar(15), convert (bigint, maxsize) * 8) + N'' KB'' end),
''growth'' = (case status & 0x100000 when 0x100000 then
convert(nvarchar(15), growth) + N''%''
else
convert(nvarchar(15), convert (bigint, growth) * 8) + N'' KB'' end),
''usage'' = (case status & 0x40 when 0x40 then ''log only'' else ''data only'' end)
from sysfiles
';

-- Identify database files that use default auto-grow properties
SELECT databasename 
      ,name 
      ,filename 
      ,growth 
	  ,fileid 
	  ,filename
	  ,filegroup
	  ,size
	  ,maxsize
	  ,growth
	  ,usage
	  FROM #DBAgentFileinfo 
ORDER BY databasename
 
-- get rid of temp table 
DROP TABLE #DBAgentFileinfo;


RAISERROR('Progress 11pc completed', 0, 1) WITH NOWAIT
RAISERROR('Most blocked queries - Should not see many queries or counts in here.. if so, the SQL has some blocking issues.', 0, 1) WITH NOWAIT
SELECT TOP 10 
[Average Time Blocked] = (total_elapsed_time - total_worker_time) / qs.execution_count ,
[Total Time Blocked] = total_elapsed_time - total_worker_time ,
[Execution count] = qs.execution_count ,
[Individual Query] = SUBSTRING (REPLACE(REPLACE(ISNULL(qt.text, ''), CHAR(13), ''), CHAR(10), ' ') ,qs.statement_start_offset/2, (
	CASE WHEN qs.statement_end_offset = -1 THEN LEN(CONVERT(NVARCHAR(MAX), qt.text)) * 2 
	ELSE qs.statement_end_offset 
	END - qs.statement_start_offset)/2) ,
[Parent Query] = REPLACE(REPLACE(ISNULL( qt.text, ''), CHAR(13), ''), CHAR(10), ' ')  ,DatabaseName = DB_NAME(qt.dbid) 
FROM sys.dm_exec_query_stats qs 
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) as qt 
ORDER BY [Average Time Blocked] DESC

-- select * from fn_my_permissions(null, 'SERVER') UNION select * from fn_my_permissions(null, 'DATABASE') order by entity_name, subentity_name, permission_name
RAISERROR('Troubleshoot permissions of current user if needed', 0, 1) WITH NOWAIT
DECLARE @docdbname VARCHAR(32);
DECLARE @tadbname VARCHAR(32);
DECLARE @cmd NVARCHAR(MAX)
DECLARE @parm NVARCHAR(100)
DECLARE @number INT
DECLARE @number_out INT
DECLARE @ktaver INT
DECLARE @bestpractices TABLE (propertyname VARCHAR(32), propertyvalue VARCHAR(32), warning VARCHAR(1000), num INT PRIMARY KEY)

RAISERROR('validate SQL/KTAs Best Practice guide settings', 0, 1) WITH NOWAIT
-- START of MAXDOP recommended setting COUNT
declare @hyperthreadingRatio bit
declare @logicalCPUs int
declare @HTEnabled int
declare @physicalCPU int
declare @SOCKET int
declare @logicalCPUPerNuma int
declare @NoOfNUMA int
declare @md int

DECLARE @ok NVARCHAR(32)
SET @ok = 'OK'

RAISERROR('Get KTA version for MAXDOP ', 0, 1) WITH NOWAIT
SELECT @cmd = N'SELECT @number_out = CONVERT(INT, SUBSTRING(version,3,1)) FROM [TotalAgility].[dbo].[db_version]'
SELECT @parm = N'@number_out INT OUTPUT'
EXECUTE sp_executesql @cmd,@parm, @number_out = @number OUTPUT
SET @ktaver = @number
PRINT N'KTA version=' +CAST(@ktaver AS VARCHAR(10))

RAISERROR('Get SQL version for MAXDOP ', 0, 1) WITH NOWAIT
DECLARE @ver INT
SET @ver = CONVERT(INT, LEFT(CONVERT( VARCHAR(32), serverproperty('productversion') ), 2))

RAISERROR('Progress 12pc completed', 0, 1) WITH NOWAIT
--TotalAgility DB Legacy CE
DECLARE @ce VARCHAR(1000)
SET @ce = 'OK'

RAISERROR('Check SQL version 12 for Legacy CE ', 0, 1) WITH NOWAIT
--For SQL 2012 or below Legacy CE
IF @ver < 12
BEGIN

    SET @ce = 'For SQL Server 2012 or below the new cardinality estimator is not applicable'

    INSERT INTO @bestpractices (propertyname, propertyvalue, warning, num)
    VALUES ('TotalAgility DB Legacy CE', CONVERT(VARCHAR(32), @number), (@ce
    ), 17)
END
RAISERROR('Check SQL version 14 for Legacy CE ', 0, 1) WITH NOWAIT
--For SQL 2014 Legacy CE
IF @ver = 12
BEGIN
    SELECT  @cmd = N'SELECT @number_out = CONVERT(INT, compatibility_level) FROM sys.databases WHERE name = db_name();'
    SELECT @parm = N'@number_out INT OUTPUT'  
    RAISERROR('Progress 13pc completed', 0, 1) WITH NOWAIT
    EXECUTE sp_executesql @cmd,@parm, @number_out = @number OUTPUT

    RAISERROR('Check SQL version 2014 for Legacy CE ', 0, 1) WITH NOWAIT
    IF @ktaver < 6 and @number != 110
    SET @ce = 'SQL Server 2014 was the first version with the new cardinality estimator and does not have the clear-cut setting that was added later in 2016.  To use the legacy cardinality estimator without the specific setting, change the compatibility level to 110, which is the level used in SQL Server 2012'

    INSERT INTO @bestpractices (propertyname, propertyvalue, warning, num)
    VALUES ('TotalAgility DB Legacy CE', CONVERT(VARCHAR(32), @number), (@ce
    ), 17)
END

--Cost threshold for parallelism
RAISERROR('Check TA Cost threshold for parallelism ', 0, 1) WITH NOWAIT
INSERT INTO @bestpractices (propertyname, propertyvalue, warning, num)
VALUES ('CTFP', CONVERT(VARCHAR(32), (SELECT value FROM	sys.configurations WITH (NOLOCK) WHERE
name = 'cost threshold for parallelism')), (CASE  
WHEN (SELECT value FROM	sys.configurations WITH (NOLOCK) WHERE
name = 'cost threshold for parallelism') = 35
THEN @ok
ELSE 'Recommended value is 35'
END
), 7)

RAISERROR('Progress 14pc completed', 0, 1) WITH NOWAIT
RAISERROR('Check SQL version 2016 for Legacy CE ', 0, 1) WITH NOWAIT
--For sql2016 or above Legacy CE
IF @ver > 12
BEGIN
    SELECT @cmd = N'SELECT @number_out = CONVERT(INT, value) FROM [TotalAgility].sys.database_scoped_configurations where name =''LEGACY_CARDINALITY_ESTIMATION'''
    SELECT @parm = N'@number_out INT OUTPUT'

    EXECUTE sp_executesql @cmd,@parm, @number_out = @number OUTPUT

    RAISERROR('Progress 15pc completed', 0, 1) WITH NOWAIT
    IF @ktaver < 6 and @number = 0
    SET @ce = 'Recommended value is 1 (Enabled)'

    INSERT INTO @bestpractices (propertyname, propertyvalue, warning, num)
    VALUES ('TotalAgility DB Legacy CE', CONVERT(VARCHAR(32), @number), (@ce
    ), 17)
END
RAISERROR('Check TotalAgility DB MAXDOP ', 0, 1) WITH NOWAIT
--TotalAgility DB MAXDOP
DECLARE @maxd VARCHAR(1000)
SET @maxd = 'OK'

RAISERROR('Progress 16pc completed', 0, 1) WITH NOWAIT
--For SQL 2014 MAXDOP
IF @ver = 12
BEGIN
    SELECT  @cmd = N'SELECT @number_out = CONVERT(INT, value) FROM sys.configurations WHERE Name= ''max degree of parallelism'''
    SELECT @parm = N'@number_out INT OUTPUT' 
    EXECUTE sp_executesql @cmd,@parm, @number_out = @number OUTPUT

    RAISERROR('Check cpu_count ', 0, 1) WITH NOWAIT
    select @logicalCPUs = cpu_count -- Logical CPU Count
        ,@hyperthreadingRatio = hyperthread_ratio --  Hyperthread Ratio
        ,@physicalCPU = cpu_count / hyperthread_ratio -- Physical CPU Count
        ,@HTEnabled = case 
            when cpu_count > hyperthread_ratio
                then 1
            else 0
            end -- HTEnabled
    from sys.dm_os_sys_info
    option (recompile);

    RAISERROR('Progress 17pc completed', 0, 1) WITH NOWAIT

    select @logicalCPUPerNuma = COUNT(parent_node_id) 
    from sys.dm_os_schedulers
    where [status] = 'VISIBLE ONLINE'
        and parent_node_id < 64
    group by parent_node_id
    option (recompile);

    select @NoOfNUMA = count(distinct parent_node_id)
    from sys.dm_os_schedulers 
    where [status] = 'VISIBLE ONLINE'
        and parent_node_id < 64

    SET @md = (select
    -- 8 or less processors and NO HT enabled
    case 
        when @logicalCPUs < 8
            and @HTEnabled = 0
            then @logicalCPUs
                -- 8 or more processors and NO HT enabled
        when @logicalCPUs >= 8
            and @HTEnabled = 0
            then 8
            -- 8 or more processors and HT enabled and NO NUMA
        when @logicalCPUs >= 8
            and @HTEnabled = 1
            and @NoofNUMA = 1
            then @logicalCPUPerNuma / @physicalCPU
                -- 8 or more processors and HT enabled and NUMA
        when @logicalCPUs >= 8
            and @HTEnabled = 1
            and @NoofNUMA > 1
            then @logicalCPUPerNuma / @physicalCPU
        else 0
    end)
    -- END of MAXDOP recommended setting COUNT
    RAISERROR('Progress 18pc completed', 0, 1) WITH NOWAIT

    --If KTA is less than 7.6 and below SQL 2014 
    IF @ktaver < 6 and @ver < 12 and @number = 1
        SET @maxd = 'It is unknown if  SQL 2012 or below the MAX DOP recommandation is applicable'

    --if KTA is less than 7.6 and SQL 2014
    IF @ktaver < 6 and @ver = 12 and @number = 1
    SET @maxd = 'OK only if Documents DB is in this SQL intance - Not recommended if KTA DB is on its own SQL intance'

    INSERT INTO @bestpractices (propertyname, propertyvalue, warning, num)
    VALUES ('TotalAgility DB MAXDOP', CONVERT(VARCHAR(32), @number), (@maxd
    ), 16)
END

RAISERROR('Check For SQL 2016 or above MAXDOP ', 0, 1) WITH NOWAIT
RAISERROR('Progress 19pc completed', 0, 1) WITH NOWAIT
--For SQL 2016 or above MAXDOP
IF @ver > 12
BEGIN

    SELECT @cmd = N'SELECT @number_out = CONVERT(INT, value) FROM [TotalAgility].sys.database_scoped_configurations where name =''MAXDOP'''

    SELECT @parm = N'@number_out INT OUTPUT'

    EXECUTE sp_executesql @cmd,@parm, @number_out = @number OUTPUT

    select @logicalCPUs = cpu_count -- Logical CPU Count
        ,@hyperthreadingRatio = hyperthread_ratio --  Hyperthread Ratio
        ,@physicalCPU = cpu_count / hyperthread_ratio -- Physical CPU Count
        ,@HTEnabled = case 
            when cpu_count > hyperthread_ratio
                then 1
            else 0
            end -- HTEnabled
    from sys.dm_os_sys_info
    option (recompile);
    RAISERROR('Progress 20pc completed', 0, 1) WITH NOWAIT
    select @logicalCPUPerNuma = COUNT(parent_node_id) -- NumberOfLogicalProcessorsPerNuma
    from sys.dm_os_schedulers
    where [status] = 'VISIBLE ONLINE'
        and parent_node_id < 64
    group by parent_node_id
    option (recompile);

    select @NoOfNUMA = count(distinct parent_node_id)
    from sys.dm_os_schedulers -- find NO OF NUMA Nodes 
    where [status] = 'VISIBLE ONLINE'
        and parent_node_id < 64

    SET @md = (select
        -- 8 or less processors and NO HT enabled
        case 
            when @logicalCPUs < 8
                and @HTEnabled = 0
                then @logicalCPUs
                    -- 8 or more processors and NO HT enabled
            when @logicalCPUs >= 8
                and @HTEnabled = 0
                then 8
                    -- 8 or more processors and HT enabled and NO NUMA
            when @logicalCPUs >= 8
                and @HTEnabled = 1
                and @NoofNUMA = 1
                then @logicalCPUPerNuma / @physicalCPU
                    -- 8 or more processors and HT enabled and NUMA
            when @logicalCPUs >= 8
                and @HTEnabled = 1
                and @NoofNUMA > 1
                then @logicalCPUPerNuma / @physicalCPU
            else 0
            end)
    -- END of MAXDOP recommended setting COUNT

    --if KTA is less than 7.6 and SQL 2016
    IF @ktaver < 6 and @ver = 13 and @number = 1
    SET @maxd = 'Microsoft default value is 0 and Documents DB MAXDOP should be set to 1'

    IF @ktaver < 6 and @ver > 13 and @number <> 0
    SET @maxd = 'Microsoft default value is 0 and Documents DB MAXDOP should be set to 1'

    INSERT INTO @bestpractices (propertyname, propertyvalue, warning, num)
    VALUES ('TotalAgility DB MAXDOP', CONVERT(VARCHAR(32), @number), (@maxd
    ), 16)
END

RAISERROR('Progress 22pc completed', 0, 1) WITH NOWAIT
--Documents DB Lecagy CE
RAISERROR('Summary of Documents DB Lecagy CE', 0, 1) WITH NOWAIT

SET @ce = 'OK'
IF @ver > 12
BEGIN
    SELECT @cmd = N'SELECT @number_out = CONVERT(INT, value) FROM [TotalAgility_Documents].sys.database_scoped_configurations where name =''LEGACY_CARDINALITY_ESTIMATION'''

    SELECT @parm = N'@number_out INT OUTPUT'

    EXECUTE sp_executesql @cmd,@parm, @number_out = @number OUTPUT

    RAISERROR('Progress 22pc completed', 0, 1) WITH NOWAIT

    IF @ktaver = 5 and @number = 0
    SET @ce = 'Recommended value is 1'

    INSERT INTO @bestpractices (propertyname, propertyvalue, warning, num)
    VALUES ('Documents DB Legacy CE', CONVERT(VARCHAR(32), @number), (@ce
    ), 19)
END
RAISERROR('Progress 23pc completed', 0, 1) WITH NOWAIT
--Documents DB MAXDOP
SET @maxd = 'OK'

IF @ver > 12
BEGIN
    SELECT @cmd = N'SELECT @number_out = CONVERT(INT, value) FROM [TotalAgility_Documents].sys.database_scoped_configurations where name =''MAXDOP'''
    SELECT @parm = N'@number_out INT OUTPUT'
    EXECUTE sp_executesql @cmd,@parm, @number_out = @number OUTPUT
    --if KTA is less than 7.6 and SQL 2016
    IF @ktaver < 6 and @ver = 13 and @number = 1
    SET @maxd = 'Microsoft default value is 0 and Documents DB MAXDOP should be set to 1'
    IF @ktaver < 6 and @ver > 13 and @number <> 0
    SET @maxd = 'Microsoft default value is 0 and Documents DB MAXDOP should be set to 1'
    INSERT INTO @bestpractices (propertyname, propertyvalue, warning, num)
    VALUES ('Documents DB MAXDOP', CONVERT(VARCHAR(32), @number), (@maxd
    ), 18)
END

RAISERROR('Progress 24pc completed', 0, 1) WITH NOWAIT
--Separator line
INSERT INTO @bestpractices (propertyname, propertyvalue, warning, num)
VALUES ('===============', 'Server properties', ('==============='
), 20)

--Documents DB on separate disk

DECLARE @tadisk nvarchar(5)
DECLARE @docdisk nvarchar(5)

set @tadbname = '[TotalAgility]'
set @docdbname = '[TotalAgility_Documents]'

SET @tadisk = LEFT((SELECT top 1 physical_name from sys.master_files where name = Replace(Replace(@tadbname, '[', ''), ']', '')), 2)
SET @docdisk = LEFT((SELECT top 1 physical_name from sys.master_files where name = Replace(Replace(@docdbname, '[', ''), ']', '')), 2)

INSERT INTO @bestpractices (propertyname, propertyvalue, warning, num)
VALUES ('DocumentsDB on separate disk', CONVERT(VARCHAR(32), (CASE  
    WHEN @tadisk = @docdisk
    THEN 'No'  
    ELSE 'Yes' 
	END)), (CASE  
    WHEN @tadisk = @docdisk
    THEN 'Not recommended'  
    ELSE @ok
	END
), 3)
RAISERROR('Progress 25pc completed', 0, 1) WITH NOWAIT
--Database and transaction logs on different drives

INSERT INTO @bestpractices (propertyname, propertyvalue, warning, num)
VALUES ('DB and LOG on different disks', CONVERT(VARCHAR(32), (CASE  
    WHEN SERVERPROPERTY('InstanceDefaultDataPath')=SERVERPROPERTY('InstanceDefaultLogPath')
    THEN 'No'  
    ELSE 'Yes' 
	END)), (CASE  
    WHEN SERVERPROPERTY('InstanceDefaultDataPath')=SERVERPROPERTY('InstanceDefaultLogPath')
    THEN 'Not recommended'  
    ELSE @ok
	END
), 4)
RAISERROR('Progress 26pc completed', 0, 1) WITH NOWAIT
--TempDB on separate drive

DECLARE @tempdisk nvarchar(5)

SET @tempdisk = LEFT((SELECT physical_name from sys.master_files where name = 'tempdev'), 2)

INSERT INTO @bestpractices (propertyname, propertyvalue, warning, num)
VALUES ('TempDB on its own disk', CONVERT(VARCHAR(32), (CASE  
    WHEN @tadisk = @tempdisk
    THEN 'No'  
    ELSE 'Yes' 
	END)), (CASE  
    WHEN @tadisk = @tempdisk
    THEN 'Not recommended'  
    ELSE @ok
	END
), 5)

--TempDB autogrowth off
RAISERROR('Progress 27pc completed', 0, 1) WITH NOWAIT
INSERT INTO @bestpractices (propertyname, propertyvalue, warning, num)
VALUES ('TempDB autogrowth OFF', CONVERT(VARCHAR(32), (CASE  
    WHEN (SELECT growth from sys.master_files where name = 'tempdev') = 0
    THEN 'Yes'  
    ELSE 'No' 
	END)), (CASE  
    WHEN (SELECT growth from sys.master_files where name = 'tempdev') = 0
    THEN @ok
    ELSE 'Not recommended'
	END
), 6)

--Server properties

DECLARE @info NVARCHAR(32)
SET @info = 'Info'

INSERT INTO @bestpractices (propertyname, propertyvalue, warning, num)
VALUES ('BuildClrVersion', CONVERT(VARCHAR(32), serverproperty('BuildClrVersion')), @info, 21)

INSERT INTO @bestpractices (propertyname, propertyvalue, warning, num)
VALUES ('Collation', CONVERT(VARCHAR(32), serverproperty('Collation')), @info, 22)

INSERT INTO @bestpractices (propertyname, propertyvalue, warning, num)
VALUES ('CollationID', CONVERT(VARCHAR(32), serverproperty('CollationID')), @info, 23)

INSERT INTO @bestpractices (propertyname, propertyvalue, warning, num)
VALUES ('ComparisonStyle', CONVERT(VARCHAR(32), serverproperty('ComparisonStyle')), @info, 24)

INSERT INTO @bestpractices (propertyname, propertyvalue, warning, num)
VALUES ('ComputerNamePhysicalNetBIOS', CONVERT(VARCHAR(32), serverproperty('ComputerNamePhysicalNetBIOS')), @info, 25)

INSERT INTO @bestpractices (propertyname, propertyvalue, warning, num)
VALUES ('Edition', CONVERT(VARCHAR(32), serverproperty('Edition')), @info, 26)

INSERT INTO @bestpractices (propertyname, propertyvalue, warning, num)
VALUES ('EditionID', CONVERT(VARCHAR(32), serverproperty('EditionID')), @info, 27)

INSERT INTO @bestpractices (propertyname, propertyvalue, warning, num)
VALUES ('EngineEdition', CONVERT(VARCHAR(32), serverproperty('EngineEdition')), @info, 28)

INSERT INTO @bestpractices (propertyname, propertyvalue, warning, num)
VALUES ('FilestreamConfiguredLevel', CONVERT(VARCHAR(32), serverproperty('FilestreamConfiguredLevel')), @info, 29)

INSERT INTO @bestpractices (propertyname, propertyvalue, warning, num)
VALUES ('FilestreamEffectiveLevel', CONVERT(VARCHAR(32), serverproperty('FilestreamEffectiveLevel')), @info, 30)

INSERT INTO @bestpractices (propertyname, propertyvalue, warning, num)
VALUES ('FilestreamShareName', CONVERT(VARCHAR(32), serverproperty('FilestreamShareName')), @info, 31)

INSERT INTO @bestpractices (propertyname, propertyvalue, warning, num)
VALUES ('InstanceName', CONVERT(VARCHAR(32), serverproperty('InstanceName')), @info, 32)

INSERT INTO @bestpractices (propertyname, propertyvalue, warning, num)
VALUES ('IsClustered', CONVERT(VARCHAR(32), serverproperty('IsClustered')), @info, 33)

INSERT INTO @bestpractices (propertyname, propertyvalue, warning, num)
VALUES ('IsFullTextInstalled', CONVERT(VARCHAR(32), serverproperty('IsFullTextInstalled')), @info, 34)

INSERT INTO @bestpractices (propertyname, propertyvalue, warning, num)
VALUES ('IsIntegratedSecurityOnly', CONVERT(VARCHAR(32), serverproperty('IsIntegratedSecurityOnly')), @info, 35)

INSERT INTO @bestpractices (propertyname, propertyvalue, warning, num)
VALUES ('IsSingleUser', CONVERT(VARCHAR(32), serverproperty('IsSingleUser')), @info, 36)

INSERT INTO @bestpractices (propertyname, propertyvalue, warning, num)
VALUES ('LCID', CONVERT(VARCHAR(32), serverproperty('LCID')), @info, 37)

INSERT INTO @bestpractices (propertyname, propertyvalue, warning, num)
VALUES ('LicenseType', CONVERT(VARCHAR(32), serverproperty('LicenseType')), @info, 38)

INSERT INTO @bestpractices (propertyname, propertyvalue, warning, num)
VALUES ('MachineName', CONVERT(VARCHAR(32), serverproperty('MachineName')), @info, 39)

INSERT INTO @bestpractices (propertyname, propertyvalue, warning, num)
VALUES ('NumLicenses', CONVERT(VARCHAR(32), serverproperty('NumLicenses')), @info, 40)

INSERT INTO @bestpractices (propertyname, propertyvalue, warning, num)
VALUES ('ProcessID', CONVERT(VARCHAR(32), serverproperty('ProcessID')), @info, 41)

INSERT INTO @bestpractices (propertyname, propertyvalue, warning, num)
VALUES ('ProductLevel', CONVERT(VARCHAR(32), serverproperty('ProductLevel')), @info, 42)

INSERT INTO @bestpractices (propertyname, propertyvalue, warning, num)
VALUES ('ProductVersion', CONVERT(VARCHAR(32), serverproperty('ProductVersion')), @info, 43)

INSERT INTO @bestpractices (propertyname, propertyvalue, warning, num)
VALUES ('ResourceLastUpdateDateTime', CONVERT(VARCHAR(32), serverproperty('ResourceLastUpdateDateTime')), @info, 44)

INSERT INTO @bestpractices (propertyname, propertyvalue, warning, num)
VALUES ('ResourceVersion', CONVERT(VARCHAR(32), serverproperty('ResourceVersion')), @info, 45)

INSERT INTO @bestpractices (propertyname, propertyvalue, warning, num)
VALUES ('ServerName', CONVERT(VARCHAR(32), serverproperty('ServerName')), @info, 46)

INSERT INTO @bestpractices (propertyname, propertyvalue, warning, num)
VALUES ('SqlCharSet', CONVERT(VARCHAR(32), serverproperty('SqlCharSet')), @info, 47)

INSERT INTO @bestpractices (propertyname, propertyvalue, warning, num)
VALUES ('SqlCharSetName', CONVERT(VARCHAR(32), serverproperty('SqlCharSetName')), @info, 48)

INSERT INTO @bestpractices (propertyname, propertyvalue, warning, num)
VALUES ('SqlSortOrder', CONVERT(VARCHAR(32), serverproperty('SqlSortOrder')), @info, 49)

INSERT INTO @bestpractices (propertyname, propertyvalue, warning, num)
VALUES ('SqlSortOrderName', CONVERT(VARCHAR(32), serverproperty('SqlSortOrderName')), @info, 50)

INSERT INTO @bestpractices (propertyname, propertyvalue, warning, num)
VALUES ('CPU Count', CONVERT(VARCHAR(32), (SELECT cpu_count from sys.dm_os_sys_info)), @info, 51)

RAISERROR('Progress 28pc completed', 0, 1) WITH NOWAIT
--if SQL server <> 2008
IF CONVERT(VARCHAR(32), serverproperty('productversion')) not like '10%'
SELECT @cmd = N'SELECT @number_out = CONVERT(INT, (SELECT physical_memory_kb from sys.dm_os_sys_info))'
ELSE
SELECT @cmd = N'SELECT @number_out = CONVERT(INT, (SELECT physical_memory_in_bytes/1024 from sys.dm_os_sys_info))'

SELECT @parm = N'@number_out INT OUTPUT'
EXECUTE sp_executesql @cmd,@parm, @number_out = @number OUTPUT

INSERT INTO @bestpractices (propertyname, propertyvalue, warning, num)
VALUES ('RAM', CONVERT(VARCHAR(32), @number), @info, 52)


SELECT propertyname as 'Server setting', propertyvalue as Value, warning as Warning from @bestpractices
ORDER by num;
RAISERROR('Progress 29pc completed', 0, 1) WITH NOWAIT


-- Basic info
SELECT * FROM [TotalAgility].[dbo].[DB_VERSION]

--System Tasks
RAISERROR('System Tasks informtion - check duedates/timeouts/intervals/status etc   ', 0, 1) WITH NOWAIT
SELECT *
FROM [TotalAgility].[dbo].WORKER_TASK AS wt WITH(NOLOCK)
WHERE SYSTEM_LEVEL=1
ORDER BY Task_type DESC

-- Summary of non-system tasks
RAISERROR('Summary of non-system tasks -- its typcially not expected to have a backlog of worker tasks ', 0, 1) WITH NOWAIT
SELECT NAME, ACTIVE, MIN(CREATED_DATE_TIME) AS MinCreated, MAX(CREATED_DATE_TIME) AS MaxCreated, COUNT(*) AS Tasks
FROM [TotalAgility].[dbo].WORKER_TASK AS wt WITH(NOLOCK)
WHERE SYSTEM_LEVEL=0
GROUP BY NAME, ACTIVE
ORDER BY Tasks DESC

-- Full of non-system worker tasks
RAISERROR('Full non-system worker tasks - can be useful to have some worker task DATA XML eg check job ids', 0, 1) WITH NOWAIT
SELECT top 1000 *
FROM [TotalAgility].[dbo].WORKER_TASK AS wt WITH(NOLOCK)
WHERE SYSTEM_LEVEL=0
ORDER BY due_date_time DESC

-- Find Large jobs in main TA DB 
RAISERROR('Large jobs in main TA DB -  over 100k history I would start to be concerned. likely caused by invalid process logic - never ending loops', 0, 1) WITH NOWAIT
SELECT jh.JOB_ID, j.JOB_STATUS, COUNT(*) AS HistoryCount
FROM [TotalAgility].[dbo].[JOB_HISTORY] AS jh with(nolock)
join [TotalAgility].[dbo].[JOB] AS j with(nolock) ON j.JOB_ID = jh.JOB_ID
GROUP BY jh.JOB_ID, j.JOB_STATUS
having  count (*) > 100000 
order by count (*) desc
RAISERROR('Progress 30pc completed', 0, 1) WITH NOWAIT
IF @ktaver < 8
	BEGIN
	RAISERROR('Find documents with highest amount of pages KTA 7.7 or below ', 0, 1) WITH NOWAIT
	select top 100 documentid, count(*) counts 
	from [TotalAgility_Documents].[dbo].[page] with(nolock)
	group by documentid
	order by counts desc  
	RAISERROR('Find folders with highest amount of documents KTA 7.7 or below  ', 0, 1) WITH NOWAIT
	select top 100 parentid, count(*) counts 
	from [TotalAgility_Documents].[dbo].[document] with(nolock)
	group by parentid
	order by counts desc    
	RAISERROR('Find larges binary data KTA 7.7 or below ', 0, 1) WITH NOWAIT
	select top 100 * from [TotalAgility_Documents].[dbo].[BinaryData] with(nolock) 
	order by length desc
END
ELSE
	BEGIN
	RAISERROR('Find documents with highest amount of pages KTA 7.8 or above', 0, 1) WITH NOWAIT
	select top 100 InternalId, id,PageCount 
	from [TotalAgility_Documents].[dbo].[DocumentData] with(nolock)
	order by PageCount desc
	RAISERROR('Find folders with highest amount of documents KTA 7.8 or above ', 0, 1) WITH NOWAIT
	select top 100 d.parentid, f.id, count(*) Count
	from [TotalAgility_Documents].[dbo].[DocumentData] As d with(nolock)
	INNER JOIN [TotalAgility_Documents].[dbo].FolderData As f with(nolock) ON d.ParentId = f.InternalId
	group by d.ParentId,f.id
	order by Count desc
	RAISERROR('Find larges binary data KTA 7.8 or above', 0, 1) WITH NOWAIT
	select top 100 id, MimeType,sessionid, lastaccessedat, isorphan,contentsource, contentreference,  convert(decimal(15,2), length / 1024.00 /1024.00) as MB from [TotalAgility_Documents].[dbo].BinaryObject with(nolock) 
	order by length desc
END
	
	
RAISERROR('Progress 31pc completed', 0, 1) WITH NOWAIT
-- Processes that contain Sleep nodes
RAISERROR('Processes that contain Sleep nodes - below 7.8, if many are active at the same time, it could comsume all (16 default) activity threads.  No other activities would be progressed at this time.', 0, 1) WITH NOWAIT
SELECT c.NAME AS CategoryName, bp.PROCESS_NAME, bp.VERSION, a.ACTIVITY_NAME, a.SLEEP_PERIOD_IN_SECONDS
FROM [TotalAgility].[dbo].BUSINESS_PROCESS AS bp WITH(NOLOCK)
INNER JOIN [TotalAgility].[dbo].ACTIVITY AS a WITH(NOLOCK) ON bp.PROCESS_ID=a.PROCESS_ID AND bp.VERSION=a.VERSION
LEFT JOIN [TotalAgility].[dbo].CATEGORY AS c WITH(NOLOCK) ON c.CATEGORY_ID=bp.CATEGORY_ID
WHERE a.ACTIVITY_TYPE=16 


RAISERROR('Progress 32pc completed', 0, 1) WITH NOWAIT
--Lingering KFS Pages data
RAISERROR('Lingering KFS Pages data - In early versions, the device clean up system task would not delete orphan binaries ', 0, 1) WITH NOWAIT
SELECT COUNT(*) AS KFSPages,
CAST(ROUND(SUM(DATALENGTH(kp.ORIGINALFILE))/1024.0/1024.0,3) AS numeric(36,3)) AS OriginalMB,
CAST(ROUND(SUM(DATALENGTH(kp.PROCESSEDFILE))/1024.0/1024.0,3) AS numeric(36,3)) AS ProcessedMB,
CAST(ROUND(SUM(DATALENGTH(kp.THUMBNAILFILE))/1024.0/1024.0,3) AS numeric(36,3)) AS ThumbMB,
CAST(ROUND(SUM(DATALENGTH(kp.PREVIEWFILE))/1024.0/1024.0,3) AS numeric(36,3)) AS PreviewMB
FROM [TotalAgility].[dbo].KFS_PAGE AS kp WITH(NOLOCK) 

--Online learning folder mappings
RAISERROR('Online learning folder mappings - used to check capture projects with OL enabled  ', 0, 1) WITH NOWAIT
SELECT co.NAME, co.VERSION, co.LAST_MODIFIED_DATE,
	CASE 
		WHEN co.OBJECT_TYPE=0 THEN 'Shared'
		WHEN co.OBJECT_TYPE=1 THEN 'Classification'
		WHEN co.OBJECT_TYPE=2 THEN 'Extraction'
	ELSE 'Unknown' END AS OBJECT_TYPE, colm.ONLINE_LEARNING_FOLDER_ID AS OLMappingFolder
FROM [TotalAgility].[dbo].[CAPTURE_ONLINE_LEARNING_MAPPING] AS colm WITH(NOLOCK)
INNER JOIN (
	SELECT co.ID, MAX(co.VERSION) AS MaxVersion 
	FROM [TotalAgility].[dbo].CAPTURE_OBJECT AS co WITH(NOLOCK)
	GROUP BY co.ID
) AS m ON m.ID=colm.CAPTURE_OBJECT_ID
INNER JOIN [TotalAgility].[dbo].CAPTURE_OBJECT AS co WITH(NOLOCK) ON colm.CAPTURE_OBJECT_ID=co.ID AND co.VERSION=m.MaxVersion
ORDER BY co.LAST_MODIFIED_DATE

RAISERROR('Progress 33pc completed', 0, 1) WITH NOWAIT
--Transformation project and training size
RAISERROR('Transformation project and training size - eg useful to know if importing packages is taking a while', 0, 1) WITH NOWAIT
SELECT co.ID, 
CASE 
	WHEN co.OBJECT_TYPE=0 THEN 'Shared'
	WHEN co.OBJECT_TYPE=1 THEN 'Classification'
	WHEN co.OBJECT_TYPE=2 THEN 'Extraction'
ELSE 'Unknown' END AS OBJECT_TYPE, co.NAME, co.VERSION,
CAST(ROUND((DATALENGTH(p.PROJECT_FOLDER_ZIP)+DATALENGTH(p.PROJECT_FILE))/1024.0/1024.0,3) AS numeric(36,3)) AS SizeMB
  FROM [TotalAgility].[dbo].[CAPTURE_OBJECT] AS co WITH(NOLOCK)
  INNER JOIN [TotalAgility].[dbo].[CAPTURE_PROJECT] AS p WITH(NOLOCK) ON (co.ID=p.ID AND co.VERSION=p.VERSION)
UNION
SELECT co.ID, 
CASE 
	WHEN co.OBJECT_TYPE=0 THEN 'Shared'
	WHEN co.OBJECT_TYPE=1 THEN 'Classification'
	WHEN co.OBJECT_TYPE=2 THEN 'Extraction'
ELSE 'Unknown' END + ' Training Set' AS OBJECT_TYPE, co.NAME, NULL AS VERSION,
CAST(ROUND((DATALENGTH(t.TRAINING_SETS_ZIP))/1024.0/1024.0,3) AS numeric(36,3)) AS SizeMB
  FROM [TotalAgility].[dbo].[CAPTURE_OBJECT] AS co WITH(NOLOCK)
  INNER JOIN [TotalAgility].[dbo].[CAPTURE_TRAINING_SETS] AS t WITH(NOLOCK) ON t.CAPTURE_OBJECT_ID=co.ID
--ORDER BY ID, VERSION
ORDER BY SizeMB DESC

RAISERROR('Progress 40pc completed', 0, 1) WITH NOWAIT
RAISERROR('To check empty folders - in early versions empty folders are not deleted by retention and could cause slow db response', 0, 1) WITH NOWAIT
IF @ktaver < 8
	BEGIN
	RAISERROR('Empty folders for KTA 7.7 and below', 0, 1) WITH NOWAIT
	select count(*) AS EmptyFolders  from [TotalAgility_Documents].[dbo].[folder] F with(nolock)
	where RootId IS NULL -- is root
	--AND Locked = 0 -- optional – in this case only unlocked folders will be deleted
	AND DocumentsOrder = '' -- no documents
	AND LastAccessedAt < dateadd(d, -2, getutcdate()) -- older than 2 day
	AND NOT EXISTS (select top 1 Id from [TotalAgility_Documents].[dbo].[folder] with(nolock) where ParentId = f.id) -- no sub folders
END
ELSE
	BEGIN
    RAISERROR('Empty folders for KTA 7.8 or above ', 0, 1) WITH NOWAIT
	select count(*) AS EmptyFolders   from [TotalAgility_Documents].[dbo].[FolderData] F with(nolock) 	
	where  NOT EXISTS (select top 1 Id from [TotalAgility_Documents].[dbo].[DocumentData] d with(nolock) where d.ParentId = f.InternalId) -- no docs 
	AND NOT EXISTS (select top 1 Id from [TotalAgility_Documents].[dbo].[FolderData] sf with(nolock) where sf.ParentId = f.InternalId) -- no sub folders
	AND f.LastAccessedAt < dateadd(d, -2, getutcdate()) -- older than 2 day
END

RAISERROR('Progress 41pc completed', 0, 1) WITH NOWAIT

RAISERROR('Retentio Policy settings - useful to know if they have unmanaged jobs/docuents and disk space issues', 0, 1) WITH NOWAIT

DECLARE @year INT
DECLARE @month INT
DECLARE @day INT
DECLARE @date datetime = getdate()
DECLARE @RetentionPolicy TABLE (RetentionPropertyName VARCHAR(64), KeepForever VARCHAR(64),PropertyValue VARCHAR(64))

INSERT INTO @RetentionPolicy (RetentionPropertyName, KeepForever, PropertyValue)
VALUES ('KeepAllFormVersions',
(SELECT  SERVER_SETTINGS.value('(/ServerSettingsServerEntity/ServerSettingsServerEntity/RetentionPolicyServerSettings/KeepAllFormVersions)[1]', 'varchar(64)') from [TotalAgility].[dbo].[server_data] with(nolock)), 
(SELECT  SERVER_SETTINGS.value('(/ServerSettingsServerEntity/ServerSettingsServerEntity/RetentionPolicyServerSettings/NumberOfFormVersions)[1]', 'varchar(64)') from [TotalAgility].[dbo].[server_data] with(nolock)));

INSERT INTO @RetentionPolicy (RetentionPropertyName, KeepForever, PropertyValue)
VALUES ('KeepAllBusinessProcessVersions',
(SELECT  SERVER_SETTINGS.value('(/ServerSettingsServerEntity/ServerSettingsServerEntity/RetentionPolicyServerSettings/KeepAllBusinessProcessVersions)[1]', 'varchar(64)') from [TotalAgility].[dbo].[server_data] with(nolock)),
(SELECT  SERVER_SETTINGS.value('(/ServerSettingsServerEntity/ServerSettingsServerEntity/RetentionPolicyServerSettings/NumberOfBusinessProcessVersions)[1]', 'varchar(64)') from [TotalAgility].[dbo].[server_data] with(nolock)));

INSERT INTO @RetentionPolicy (RetentionPropertyName, KeepForever, PropertyValue)
VALUES ('KeepAllBusinessRuleVersions',
(SELECT  SERVER_SETTINGS.value('(/ServerSettingsServerEntity/ServerSettingsServerEntity/RetentionPolicyServerSettings/KeepAllBusinessRuleVersions)[1]', 'varchar(64)') from [TotalAgility].[dbo].[server_data] with(nolock)),
(SELECT  SERVER_SETTINGS.value('(/ServerSettingsServerEntity/ServerSettingsServerEntity/RetentionPolicyServerSettings/NumberOfBusinessRuleVersions)[1]', 'varchar(64)') from [TotalAgility].[dbo].[server_data] with(nolock)));

INSERT INTO @RetentionPolicy (RetentionPropertyName, KeepForever, PropertyValue)
VALUES ('KeepAllExtractionGroupVersions',
(SELECT  SERVER_SETTINGS.value('(/ServerSettingsServerEntity/ServerSettingsServerEntity/RetentionPolicyServerSettings/KeepAllExtractionGroupVersions)[1]', 'varchar(64)') from [TotalAgility].[dbo].[server_data] with(nolock)),
(SELECT  SERVER_SETTINGS.value('(/ServerSettingsServerEntity/ServerSettingsServerEntity/RetentionPolicyServerSettings/NumberOfExtractionGroupVersions)[1]', 'varchar(64)') from [TotalAgility].[dbo].[server_data] with(nolock)));

INSERT INTO @RetentionPolicy (RetentionPropertyName, KeepForever, PropertyValue)
VALUES ('KeepAllClassificationGroupVersions',
(SELECT  SERVER_SETTINGS.value('(/ServerSettingsServerEntity/ServerSettingsServerEntity/RetentionPolicyServerSettings/KeepAllClassificationGroupVersions)[1]', 'varchar(64)') from [TotalAgility].[dbo].[server_data] with(nolock)),
(SELECT  SERVER_SETTINGS.value('(/ServerSettingsServerEntity/ServerSettingsServerEntity/RetentionPolicyServerSettings/NumberOfClassificationGroupVersions)[1]', 'varchar(64)') from [TotalAgility].[dbo].[server_data] with(nolock)));

INSERT INTO @RetentionPolicy (RetentionPropertyName, KeepForever, PropertyValue)
VALUES ('KeepInternalUsersForever',
(SELECT  SERVER_SETTINGS.value('(/ServerSettingsServerEntity/ServerSettingsServerEntity/RetentionPolicyServerSettings/KeepInternalUsersForever)[1]', 'varchar(64)') from [TotalAgility].[dbo].[server_data] with(nolock)),
(SELECT  SERVER_SETTINGS.value('(/ServerSettingsServerEntity/ServerSettingsServerEntity/RetentionPolicyServerSettings/InternalUserRetentionPeriod)[1]', 'varchar(64)') from [TotalAgility].[dbo].[server_data] with(nolock)));

INSERT INTO @RetentionPolicy (RetentionPropertyName, KeepForever, PropertyValue)
VALUES ('KeepExternalUsersForever',
(SELECT  SERVER_SETTINGS.value('(/ServerSettingsServerEntity/ServerSettingsServerEntity/RetentionPolicyServerSettings/KeepExternalUsersForever)[1]', 'varchar(64)') from [TotalAgility].[dbo].[server_data] with(nolock)),
(SELECT  SERVER_SETTINGS.value('(/ServerSettingsServerEntity/ServerSettingsServerEntity/RetentionPolicyServerSettings/ExternalUserRetentionPeriod)[1]', 'varchar(64)') from [TotalAgility].[dbo].[server_data] with(nolock)));

INSERT INTO @RetentionPolicy (RetentionPropertyName, KeepForever, PropertyValue)
VALUES ('KeepAuditLogForever',
(SELECT  SERVER_SETTINGS.value('(/ServerSettingsServerEntity/ServerSettingsServerEntity/RetentionPolicyServerSettings/KeepAuditLogForever)[1]', 'varchar(64)') from [TotalAgility].[dbo].[server_data] with(nolock)),
(SELECT  SERVER_SETTINGS.value('(/ServerSettingsServerEntity/ServerSettingsServerEntity/RetentionPolicyServerSettings/AuditLogRetentionPeriod)[1]', 'varchar(64)') from [TotalAgility].[dbo].[server_data] with(nolock)));

INSERT INTO @RetentionPolicy (RetentionPropertyName, KeepForever, PropertyValue)
VALUES ('KeepDocumentsForever',
(SELECT  SERVER_SETTINGS.value('(/ServerSettingsServerEntity/ServerSettingsServerEntity/RetentionPolicyServerSettings/KeepDocumentsForever)[1]', 'varchar(64)') from [TotalAgility].[dbo].[server_data] with(nolock)),
(SELECT  SERVER_SETTINGS.value('(/ServerSettingsServerEntity/ServerSettingsServerEntity/RetentionPolicyServerSettings/DocumentsRetentionPeriod)[1]', 'varchar(64)') from [TotalAgility].[dbo].[server_data] with(nolock)));

INSERT INTO @RetentionPolicy (RetentionPropertyName, KeepForever, PropertyValue)
VALUES ('KeepCCMPacksForever',
(SELECT  SERVER_SETTINGS.value('(/ServerSettingsServerEntity/ServerSettingsServerEntity/RetentionPolicyServerSettings/KeepCCMPacksForever)[1]', 'varchar(64)') from [TotalAgility].[dbo].[server_data] with(nolock)),
(SELECT  SERVER_SETTINGS.value('(/ServerSettingsServerEntity/ServerSettingsServerEntity/RetentionPolicyServerSettings/CCMPacksRetentionPeriod)[1]', 'varchar(64)') from [TotalAgility].[dbo].[server_data] with(nolock)));

INSERT INTO @RetentionPolicy (RetentionPropertyName, KeepForever, PropertyValue)
VALUES ('MaxNumberOfDocumentsToDelete',null,
(SELECT  SERVER_SETTINGS.value('(/ServerSettingsServerEntity/ServerSettingsServerEntity/RetentionPolicyServerSettings/MaxNumberOfDocumentsToDelete)[1]', 'varchar(64)') from [TotalAgility].[dbo].[server_data] with(nolock)));

INSERT INTO @RetentionPolicy (RetentionPropertyName, KeepForever, PropertyValue)
VALUES ('MaxNumberOfDocumentsToDelete', null,
(SELECT  SERVER_SETTINGS.value('(/ServerSettingsServerEntity/ServerSettingsServerEntity/RetentionPolicyServerSettings/MaxNumberOfJobsToDelete)[1]', 'varchar(64)') from [TotalAgility].[dbo].[server_data] with(nolock)));

select * from @RetentionPolicy

RAISERROR('Number of jobs to delete with Retention Policy enabled', 0, 1) WITH NOWAIT

DECLARE @TempBusinessProcessIds TABLE (Id binary(16))
DECLARE @NumberRecords int, @RowCount int, @ID binary(16)
DECLARE @FinJobRetentionPolicy TABLE (ProcessName NVARCHAR(64), KeepForever VARCHAR(64),RetentionDate datetime, NumberOfFinJobs numeric )

INSERT INTO @TempBusinessProcessIds 
SELECT b.PROCESS_ID FROM [TotalAgility].[dbo].[BUSINESS_PROCESS] AS b WITH (NOLOCK) where process_type = 0 
and version = (select max(version) from business_process where process_id = b.process_id)  
group by PROCESS_ID

-- Get the number of records in the temporary table
select @NumberRecords = COUNT(*) from @TempBusinessProcessIds
PRINT   @NumberRecords 

WHILE @NumberRecords > 0
BEGIN
    SET @ID = (SELECT TOP 1 id FROM @TempBusinessProcessIds);
	SET @year =0
    SET @month =0
    SET @day =0
    SET @date = GETDATE()
    SET @year = (SELECT top 1  SETTINGS_XML.value('(/BusinessProcessSettings/RetentionPolicySettings/RetentionPolicyDuration/Years)[1]', 'INT') from [TotalAgility].[dbo].[BUSINESS_PROCESS] with(nolock) where process_id = @id)
    SET @month = (SELECT top 1  SETTINGS_XML.value('(/BusinessProcessSettings/RetentionPolicySettings/RetentionPolicyDuration/Months)[1]', 'INT') from [TotalAgility].[dbo].[BUSINESS_PROCESS] with(nolock) where process_id = @id)
    SET @day = (SELECT top 1  SETTINGS_XML.value('(/BusinessProcessSettings/RetentionPolicySettings/RetentionPolicyDuration/Days)[1]', 'INT') from [TotalAgility].[dbo].[BUSINESS_PROCESS] with(nolock) where process_id = @id)
    SET @date = DATEADD(YEAR,-@year,@date);
    SET @date = DATEADD(MONTH,-@month,@date);
    SET @date = DATEADD(DAY,-@day,@date);
 	INSERT INTO @FinJobRetentionPolicy (ProcessName, KeepForever, RetentionDate,NumberOfFinJobs)
	VALUES 
	(
	(SELECT top 1 process_name from [TotalAgility].[dbo].BUSINESS_PROCESS with(nolock) where process_id = @id)
	,(SELECT top 1  SETTINGS_XML.value('(/BusinessProcessSettings/RetentionPolicySettings/KeepForever)[1]', 'varchar(64)') from [TotalAgility].[dbo].[BUSINESS_PROCESS] with(nolock) where process_id = @id)
	,@date
    ,(SELECT  count(*) from [TotalAgility_FinishedJobs].[dbo].Finished_job with(nolock) where FINISH_TIME < @date and process_id = @id)
	);
	DELETE  FROM @TempBusinessProcessIds WHERE id = @id
    -- DECREMENT THE COUNT BY 1
	SET @NumberRecords =  @NumberRecords - 1;
END

select  * from @FinJobRetentionPolicy

RAISERROR('Retention Policy Failures', 0, 1) WITH NOWAIT
select * FROM [TotalAgility].[dbo].[RETENTION_POLICY_FAILURE] with(nolock)


RAISERROR('Remaining Document Retention Policy', 0, 1) WITH NOWAIT

SET @year =0
SET @month =0
SET @day =0
SET @date = GETDATE()

SET @year = (SELECT  SERVER_SETTINGS.value('(/ServerSettingsServerEntity/ServerSettingsServerEntity/RetentionPolicyServerSettings/DocumentsRetentionPeriod/Years)[1]', 'INT')
from [TotalAgility].[dbo].[server_data] with(nolock))
 
SET @month = (SELECT  SERVER_SETTINGS.value('(/ServerSettingsServerEntity/ServerSettingsServerEntity/RetentionPolicyServerSettings/DocumentsRetentionPeriod/Months)[1]', 'INT')
from [TotalAgility].[dbo].[server_data] with(nolock))

SET @day = (SELECT  SERVER_SETTINGS.value('(/ServerSettingsServerEntity/ServerSettingsServerEntity/RetentionPolicyServerSettings/DocumentsRetentionPeriod/Days)[1]', 'INT')
from [TotalAgility].[dbo].[server_data] with(nolock))
 
PRINT N'Document Retention year=' +CAST(@year AS VARCHAR(10)) 
PRINT N'Document Retention month=' +CAST(@month AS VARCHAR(10)) 
PRINT N'Document Retention dat=' +CAST(@day AS VARCHAR(10)) 

SET @date = DATEADD(YEAR,-@year,@date);
SET @date = DATEADD(MONTH,-@month,@date);
SET @date = DATEADD(DAY,-@day,@date);

PRINT N'Document Retention date=' +CAST(@date AS VARCHAR(10)) 
PRINT N'KTA version=' +CAST(@ktaver AS VARCHAR(10)) 
 
IF @ktaver < 8
BEGIN
    RAISERROR('Remaining document retention data per month and totals for KTA 7.7 or below', 0, 1) WITH NOWAIT
    SELECT DATEPART(YEAR,d.LastAccessedAt) AS [LastAccessed Year], DATEPART(MONTH,d.LastAccessedAt) AS [LastAccessed Month],
    CAST(ROUND(SUM(DATALENGTH(bs.Contents))/1024.0/1024.0,3) AS numeric(36,3)) AS SizeMb --f.DocumentId, f.FileId
    FROM [TotalAgility_Documents].[dbo].[Document] AS d with(nolock)
    LEFT JOIN
    (
    -- Doc source file
    SELECT d.Id AS DocumentId, d.FileId
    FROM [TotalAgility_Documents].[dbo].[Document] AS d with(nolock)
    UNION
    -- Doc extensions
    SELECT de.DocumentId, e.FileId
    FROM [TotalAgility_Documents].[dbo].[DocumentExtension] AS de with(nolock)
    INNER JOIN [TotalAgility_Documents].[dbo].[Extension] AS e ON e.ExtensionId=de.ExtensionId
    UNION
    -- Page files
    SELECT p.DocumentId, p.FileId
    FROM [TotalAgility_Documents].[dbo].[Page] AS p  with(nolock)
    UNION
    -- Page extensions
    SELECT p.DocumentId, e.FileId
    FROM [TotalAgility_Documents].[dbo].[PageExtension] AS pe with(nolock)
    INNER JOIN [TotalAgility_Documents].[dbo].[Extension] AS e ON e.ExtensionId=pe.ExtensionId
    INNER JOIN [TotalAgility_Documents].[dbo].[Page] AS p ON p.PageId=pe.PageId
    UNION
    -- OCR
    SELECT ocr.DocumentId, ocr.TextLinesId AS FileId
            FROM [TotalAgility_Documents].[dbo].[Page] AS ocr with(nolock)
    ) AS f ON f.DocumentId=d.Id
    INNER JOIN [TotalAgility_Documents].[dbo].[BinaryData] AS bd ON f.FileId=bd.FileId
    INNER JOIN [TotalAgility_Documents].[dbo].[BlobStorage] AS bs ON bs.BlobId=bd.SqlPath
    WHERE d.LastAccessedAt < @date
    GROUP BY DATEPART(YEAR,d.LastAccessedAt), DATEPART(MONTH,d.LastAccessedAt) WITH ROLLUP
    ORDER BY [LastAccessed Year] DESC, [LastAccessed Month] DESC
END
ELSE
BEGIN
    RAISERROR('Note:this may contain non document data (7.8 or above pages/document association is encoded) - Remaining data per month and totals for KTA 7.8 or above ', 0, 1) WITH NOWAIT
    SELECT DATEPART(YEAR,bd.LastAccessedAt) AS [LastAccessed Year], DATEPART(MONTH,bd.LastAccessedAt) AS [LastAccessed Month],
    CAST(ROUND(SUM(DATALENGTH(bd.Content))/1024.0/1024.0,3) AS numeric(36,3)) AS MB
    FROM [TotalAgility_Documents].[dbo].[BinaryObject] AS bd WITH(NOLOCK)
    WHERE bd.LastAccessedAt < @date
    GROUP BY DATEPART(YEAR,bd.LastAccessedAt), DATEPART(MONTH,bd.LastAccessedAt) WITH ROLLUP
    ORDER BY [LastAccessed Year] DESC, [LastAccessed Month] DESC
END

RAISERROR('Job History time pending ', 0, 1) WITH NOWAIT
declare @column varchar(50)
declare @sql  nvarchar(max)
print 'Job History time pending KTA version ='+ CAST(@ktaver AS varchar(100))
IF @ktaver > 3
	BEGIN		
		set @column='automatic'
		set @sql ='select job_id, set_time, activity_type, node_name, embedded_process_count, process_name,node_id,time_pending_in_secs, TIME_SPENT_IN_SECONDS, version 
		from 
		( select top(100) job_id, set_time, activity_type, node_name, embedded_process_count, process_name,node_id,time_pending_in_secs, TIME_SPENT_IN_SECONDS, version 
		from [TotalAgility].[dbo].[job_history] with(nolock)  where set_time > dateadd (month, -1,getdate()) and '+ @column +' in (1,2) order by time_pending_in_secs desc
		union all
		select top(100) job_id, set_time, activity_type, node_name, embedded_process_count, process_name,node_id,time_pending_in_secs, TIME_SPENT_IN_SECONDS, version 
		from [TotalAgility_FinishedJobs].[dbo].[finished_job_history] with(nolock)  where set_time > dateadd (month, -1,getdate()) and  '+ @column +' in (1,2)order by time_pending_in_secs desc
			) jobHistory
		order by time_pending_in_secs DESC'
		print @column 
		print @sql
		execute(@sql)
	END
	ELSE
	BEGIN
		--The below query will get a list of top Transformation Server activities processed in the past week order by pending time.
		RAISERROR('Job History time pending ', 0, 1) WITH NOWAIT
		select job_id, set_time, activity_type, node_name, embedded_process_count, process_name,node_id,time_pending_in_secs, TIME_SPENT_IN_SECONDS, version 
		from 
		( select top(100) job_id, set_time, activity_type, node_name, embedded_process_count, process_name,node_id,time_pending_in_secs, TIME_SPENT_IN_SECONDS, version 
		from [TotalAgility].[dbo].[job_history] with(nolock)  where set_time > dateadd (month, -1,getdate()) and ACTIVITY_TYPE = 4096 order by time_pending_in_secs desc
		union all
		select top(100) job_id, set_time, activity_type, node_name, embedded_process_count, process_name,node_id,time_pending_in_secs, TIME_SPENT_IN_SECONDS, version 
		from [TotalAgility_FinishedJobs].[dbo].[finished_job_history] with(nolock)  where set_time > dateadd (month, -1,getdate()) and ACTIVITY_TYPE = 4096 order by time_pending_in_secs desc
			) jobHistory
		order by time_pending_in_secs DESC
	END
RAISERROR('Progress 41pc completed', 0, 1) WITH NOWAIT
RAISERROR('Job History time pending ', 0, 1) WITH NOWAIT
print 'Job History time spent KTA version ='+ CAST(@ktaver AS varchar(100))
IF @ktaver > 3
BEGIN
	set @column='automatic'
	set @sql ='select job_id, set_time, activity_type, node_name, embedded_process_count, process_name,node_id,time_pending_in_secs, TIME_SPENT_IN_SECONDS, version 
	from 
	( select top(100) job_id, set_time, activity_type, node_name, embedded_process_count, process_name,node_id,time_pending_in_secs, TIME_SPENT_IN_SECONDS, version 
	from [TotalAgility].[dbo].job_history with(nolock) where set_time > dateadd (month, -1,getdate()) and  '+ @column +'  in (1,2) order by TIME_SPENT_IN_SECONDS desc
	union all
	select top(100) job_id, set_time, activity_type, node_name, embedded_process_count, process_name,node_id,time_pending_in_secs, TIME_SPENT_IN_SECONDS, version 
	from [TotalAgility_FinishedJobs].[dbo].finished_job_history with(nolock)  where set_time > dateadd (month, -1,getdate()) and  '+ @column +'  in (1,2) order by TIME_SPENT_IN_SECONDS desc
		) jobHistory
	order by TIME_SPENT_IN_SECONDS DESC'
    print @column 
	print @sql
	execute(@sql)
END
ELSE
BEGIN
	-- The below query will get a list of top Transformation Server activities processed in the past week order by taken time.
	RAISERROR('Job History taken spent ', 0, 1) WITH NOWAIT
	select job_id, set_time, activity_type, node_name, embedded_process_count, process_name,node_id,time_pending_in_secs, TIME_SPENT_IN_SECONDS, version 
	from 
	( select top(100) job_id, set_time, activity_type, node_name, embedded_process_count, process_name,node_id,time_pending_in_secs, TIME_SPENT_IN_SECONDS, version 
	from [TotalAgility].[dbo].job_history with(nolock) where set_time > dateadd (month, -1,getdate()) and ACTIVITY_TYPE = 4096 order by TIME_SPENT_IN_SECONDS desc
	union all
	select top(100) job_id, set_time, activity_type, node_name, embedded_process_count, process_name,node_id,time_pending_in_secs, TIME_SPENT_IN_SECONDS, version 
	from [TotalAgility_FinishedJobs].[dbo].finished_job_history with(nolock)  where set_time > dateadd (month, -1,getdate()) and ACTIVITY_TYPE = 4096 order by TIME_SPENT_IN_SECONDS desc
		) jobHistory
	order by TIME_SPENT_IN_SECONDS DESC
END
RAISERROR('Progress 43pc completed', 0, 1) WITH NOWAIT


RAISERROR('Get Pending Live Auto Activities by status - This query will get a list of Live Auto Activities grouped by status, so you can check what acticities are in the pending/locked/taken/suspended  ', 0, 1) WITH NOWAIT
select P.POOLNAME, PROCESS_NAME, NODE_NAME, TYPE,
CASE ACTIVITY_STATUS
WHEN -1 THEN 'Completed'
WHEN 0 THEN 'Pending'
WHEN 1 THEN 'Active (In Progress)'
WHEN 2 THEN 'Offered'
WHEN 3 THEN 'Suspended'
WHEN 4 THEN 'Locked'
WHEN 5 THEN 'Pending Completion'
WHEN 7 THEN 'On Hold'
WHEN 8 THEN 'Awaiting Event'
WHEN 10 THEN 'Saved'
END AS ACTIVITY_STATUS, AUTOMATIC, PRIORITY, VERSION, count(1) as COUNT,
convert(varchar(19),min(DUE_DATE),120) as MIN, convert(varchar(19),max(DUE_DATE),120) as MAX
from [TotalAgility].[dbo].[LIVE_ACTIVITY] L
inner join [TotalAgility].[dbo].[THREADPOOL] P
on P.POOLID = L.POOLID
where AUTOMATIC in (1,2,3)
group by P.POOLNAME, PROCESS_NAME, NODE_NAME, ACTIVITY_STATUS, AUTOMATIC, PRIORITY, VERSION, TYPE
order by count desc
RAISERROR('Progress 44pc completed', 0, 1) WITH NOWAIT
--The below query will get a list of Threadpools
RAISERROR('List of Threadpools - this is the number of acticity threads  - how many activities each core worker can take concurrently', 0, 1) WITH NOWAIT
select * from [TotalAgility].[dbo].[THREADPOOL] P

RAISERROR('Progress 45pc completed', 0, 1) WITH NOWAIT
--All orphans per month and totals
IF @ktaver < 8
	BEGIN
	RAISERROR('All orphans per month and totals for KTA 7.7 or below', 0, 1) WITH NOWAIT
	SELECT DATEPART(YEAR,bd.CreationDate) AS [Year], DATEPART(MONTH,bd.CreationDate) AS [Month], COUNT(*) AS Orphans,
	CAST(ROUND(SUM(DATALENGTH(bs.Contents))/1024.0/1024.0,3) AS numeric(36,3)) AS MB
	FROM [TotalAgility_Documents].[dbo].[BinaryData] AS bd WITH(NOLOCK)
	INNER JOIN [TotalAgility_Documents].[dbo].[BlobStorage] AS bs WITH(NOLOCK) ON bd.SqlPath=bs.BlobId
	WHERE bd.IsOrphan=1 and bd.IsCommitted = 1
	GROUP BY DATEPART(YEAR,bd.CreationDate), DATEPART(MONTH,bd.CreationDate) WITH ROLLUP
	ORDER BY [Year] DESC, [Month] DESC
	END
ELSE
	BEGIN
	RAISERROR('All orphans per month and totals for KTA 7.8 or above ', 0, 1) WITH NOWAIT
	SELECT DATEPART(YEAR,bd.LastAccessedAt) AS [LastAccessed Year], DATEPART(MONTH,bd.LastAccessedAt) AS [LastAccessed Month], COUNT(*) AS Orphans,
	CAST(ROUND(SUM(DATALENGTH(bd.Content))/1024.0/1024.0,3) AS numeric(36,3)) AS MB
	FROM [TotalAgility_Documents].[dbo].[BinaryObject] AS bd WITH(NOLOCK)
	WHERE bd.IsOrphan=1 
	GROUP BY DATEPART(YEAR,bd.LastAccessedAt), DATEPART(MONTH,bd.LastAccessedAt) WITH ROLLUP
	ORDER BY [LastAccessed Year] DESC, [LastAccessed Month] DESC
	END

IF @ktaver < 8  
	BEGIN
    RAISERROR('Detached binaries: data not marked as orphan, but not attached to any record - KTA 7.7 or below ', 0, 1) WITH NOWAIT
    select CAST(ROUND(SUM((b.Length))/1024.0/1024.0,2) AS numeric(36,2)) AS MB, COUNT(*) AS DetachedBinaries
    FROM [TotalAgility_Documents].[dbo].BinaryData b WITH(NOLOCK)
    WHERE IsOrphan = 0
    AND NOT EXISTS (SELECT 1 FROM [TotalAgility_Documents].[dbo].Page WITH(NOLOCK) WHERE FileId = b.FileId)
    AND NOT EXISTS (SELECT 1 FROM [TotalAgility_Documents].[dbo].Page WITH(NOLOCK) WHERE TextLinesId = b.FileId)
    AND NOT EXISTS (SELECT 1 FROM [TotalAgility_Documents].[dbo].Document WITH(NOLOCK) WHERE FileId = b.FileId)
    AND NOT EXISTS (SELECT 1 FROM [TotalAgility_Documents].[dbo].Extension WITH(NOLOCK) WHERE FileId = b.FileId)
	END
	
RAISERROR('latest job notes with errors - just to get a quick snap shot of why recent jobs are suspending ', 0, 1) WITH NOWAIT
select JOB_id, RESOURCE_ID, CREATION_DATE, CREATION_DATE_MILLISECS, NOTE_TYPE_ID, JOB_NOTE  
from
(select top 100 JOB_id, RESOURCE_ID, CREATION_DATE, CREATION_DATE_MILLISECS, NOTE_TYPE_ID, JOB_NOTE  
from [TotalAgility]..[job_note] with(nolock) 
where  job_note like '%error%' order by CREATION_DATE desc
union all 
select top 100 JOB_id, RESOURCE_ID, CREATION_DATE, CREATION_DATE_MILLISECS, NOTE_TYPE_ID, JOB_NOTE  
from [TotalAgility_FinishedJobs]..[finished_job_note] with(nolock)
where  job_note like '%error%' order by CREATION_DATE desc
) jobnotes
order by CREATION_DATE desc 

RAISERROR('Progress 46pc completed', 0, 1) WITH NOWAIT
RAISERROR('Top 25 queries from SQL cache worker time - Good to know if SQL has sub optimal plans', 0, 1) WITH NOWAIT
SELECT TOP (25)  CONVERT(VARCHAR(30),DB_NAME(qp.dbid)) AS DatabaseName, REPLACE(REPLACE(ISNULL( st.text, ''), CHAR(13), ''), CHAR(10), ' ') AS Query, qs.total_worker_time, qs.execution_count, qs.max_elapsed_time, qs.max_worker_time,
qs.last_grant_kb, qs.last_ideal_grant_kb, qs.max_grant_kb, qs.max_ideal_grant_kb, qs.max_rows, qs.min_used_grant_kb, qs.total_used_grant_kb, qp.query_plan
FROM sys.dm_exec_query_stats AS qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) AS qp
WHERE qs.max_worker_time > 300
OR qs.max_elapsed_time > 300
order by qs.max_worker_time  desc

RAISERROR('Progress 47pc completed', 0, 1) WITH NOWAIT
RAISERROR('Top 25 queries from SQL cache by max grant size -  Good to know if SQL has sub optimal plans', 0, 1) WITH NOWAIT
SELECT TOP (25)  CONVERT(VARCHAR(30),DB_NAME(qp.dbid)) AS DatabaseName, REPLACE(REPLACE(ISNULL( st.text, ''), CHAR(13), ''), CHAR(10), ' ') AS Query, qs.total_worker_time, qs.execution_count, qs.max_elapsed_time, qs.max_worker_time,
qs.last_grant_kb, qs.last_ideal_grant_kb, qs.max_grant_kb, qs.max_ideal_grant_kb, qs.max_rows, qs.min_used_grant_kb, qs.total_used_grant_kb, qp.query_plan
FROM sys.dm_exec_query_stats AS qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) AS qp
WHERE qs.max_worker_time > 300
OR qs.max_elapsed_time > 300
order by qs.max_grant_kb  desc

RAISERROR('Progress 48pc completed', 0, 1) WITH NOWAIT
RAISERROR('Get Online learning details from database to see if OL is functional or has a backlog of samples ..  if the OL temp has many samples, it means TS is unable to progress them and can lead to SQL contention', 0, 1) WITH NOWAIT
IF @ktaver < 8
        BEGIN
        RAISERROR('Get Online learning 7.7 or below details from database ', 0, 1) WITH NOWAIT
        SELECT olroot.Id AS OLRoot, co.NAME,
        CASE 
        WHEN co.OBJECT_TYPE=0 THEN 'Shared'
        WHEN co.OBJECT_TYPE=1 THEN 'Classification'
        WHEN co.OBJECT_TYPE=2 THEN 'Extraction'
        ELSE 'Unknown' END AS OBJECT_TYPE, 
        ns.NewSampleRoot, olmap.OLMapping AS OLMappingReadyToTrain, temp.TempHolding, 
        (olmap.ReadyToTrainCount + ns.NewSampleCount + temp.TempHoldingCount) AS TotalDocs,
        ns.NewSampleCount, olmap.ReadyToTrainCount, temp.TempHoldingCount
        FROM [TotalAgility_Documents].[dbo].[Folder] AS olroot
        LEFT JOIN (--New Samples by OL Root
        SELECT olr.Id AS OLRoot, nsb.ParentId AS NewSampleRoot, (SELECT COUNT(*) FROM [TotalAgility_Documents].[dbo].[Folder] AS n INNER JOIN [TotalAgility_Documents].[dbo].[Document] AS d ON d.ParentId=n.Id WHERE n.ParentId=nsb.ParentId) AS NewSampleCount
        FROM [TotalAgility_Documents].[dbo].[Folder] AS olr
        INNER JOIN [TotalAgility_Documents].[dbo].[Folder] AS nsb ON nsb.RootId=olr.Id AND nsb.TypeId='Id10' --20 block folders for new samples
        WHERE olr.TypeId='Id1'
        GROUP BY olr.Id, nsb.ParentId
        ) AS ns ON ns.OLRoot=olroot.Id
        LEFT JOIN (-- Temp holding docs by OL Root
        SELECT olr.Id AS OLRoot, temp.Id AS TempHolding, (SELECT COUNT(*) FROM [TotalAgility_Documents].[dbo].[Document] AS d WHERE d.ParentId=temp.Id) AS TempHoldingCount
        FROM [TotalAgility_Documents].[dbo].[Folder] AS olr
        INNER JOIN [TotalAgility_Documents].[dbo].Folder AS temp ON temp.RootId=olr.Id AND temp.NamingCustomText='Temp'
        WHERE olr.TypeId='Id1'
        GROUP BY olr.Id, temp.Id
        ) AS temp ON temp.OLRoot=olroot.Id
        LEFT JOIN (-- OLMapping/Ready-to-train docs by OL Root
        SELECT olroot.Id AS OLRoot, rtt.Id AS OLMapping, (SELECT COUNT(*) FROM [TotalAgility_Documents].[dbo].[Document] AS d WHERE d.ParentId=rtt.Id) AS ReadyToTrainCount
        FROM [TotalAgility_Documents].[dbo].[Folder] AS olroot
        INNER JOIN [TotalAgility_Documents].[dbo].[Folder] AS rtt ON rtt.RootId=olroot.Id AND rtt.NamingCustomText='Train'
        WHERE olroot.TypeId='Id1'
        GROUP BY olroot.Id, rtt.Id
        ) AS olmap ON olmap.OLRoot=olroot.Id
        LEFT JOIN [TotalAgility].[dbo].[CAPTURE_ONLINE_LEARNING_MAPPING] AS olm 
        ON REPLACE(CONVERT(VARCHAR(36), olmap.OLMapping,2),'-','')=CONVERT(VARCHAR(36), olm.ONLINE_LEARNING_FOLDER_ID,2)
        LEFT JOIN (
        SELECT co.ID, MAX(co.VERSION) AS MaxVersion 
        FROM [TotalAgility].[dbo].[CAPTURE_OBJECT] AS co
        GROUP BY co.ID
        ) AS m ON m.ID=olm.CAPTURE_OBJECT_ID
        LEFT JOIN [TotalAgility].[dbo].[CAPTURE_OBJECT] AS co ON (olm.CAPTURE_OBJECT_ID=co.ID AND co.VERSION=m.MaxVersion)
        WHERE olroot.TypeId='Id1' --OL Root folder type
        END
    ELSE        
        BEGIN
        RAISERROR('Get Online learning 7.8 or above details from database ', 0, 1) WITH NOWAIT
        SELECT olroot.id AS olrootid, co.NAME,
        CASE 
        WHEN co.OBJECT_TYPE=0 THEN 'Shared'
        WHEN co.OBJECT_TYPE=1 THEN 'Classification'
        WHEN co.OBJECT_TYPE=2 THEN 'Extraction'
        ELSE 'Unknown' END AS OBJECT_TYPE, 
        ns.NewSampleRoot, olmap.ReadyToTrainid AS OLMappingReadyToTrain, temp.TempHolding, 
        (olmap.ReadyToTrainCount + ns.NewSampleCount + temp.TempHoldingCount) AS TotalDocs,
        ns.NewSampleCount, olmap.ReadyToTrainCount, temp.TempHoldingCount
        FROM [TotalAgility_Documents].[dbo].[FolderData] AS olroot
        LEFT JOIN (--New Samples by OL Root                                                      
        SELECT olr.id AS OLRoot, nsb.id AS NewSampleRoot, (SELECT COUNT(*) FROM [TotalAgility_Documents].[dbo].FolderData AS n INNER JOIN [TotalAgility_Documents].[dbo].Documentdata AS d ON d.ParentId=n.InternalId WHERE n.ParentId=nsb.ParentId) AS NewSampleCount   
        FROM [TotalAgility_Documents].[dbo].[FolderData] AS olr  
        INNER JOIN [TotalAgility_Documents].[dbo].[FolderData] AS nsb ON nsb.RootId=olr.InternalId And nsb.folderIndex=0 AND nsb.ParentId !=olr.InternalId
        GROUP BY nsb.id, olr.id, nsb.ParentId
        ) AS ns ON ns.OLRoot=olroot.id
        LEFT JOIN (-- Temp holding docs by OL Root
        SELECT olr.Id AS OLRoot, temp.Id AS TempHolding, (SELECT COUNT(*) FROM [TotalAgility_Documents].[dbo].DocumentData AS d WHERE d.ParentId=temp.InternalId) AS TempHoldingCount
        FROM [TotalAgility_Documents].[dbo].FolderData AS olr
        INNER JOIN [TotalAgility_Documents].[dbo].FolderData AS temp ON temp.ParentId=olr.InternalId AND temp.folderIndex=1
        GROUP BY olr.Id, temp.Id, temp.InternalId
        ) AS temp ON temp.OLRoot=olroot.Id
        LEFT JOIN (-- OLMapping/Ready-to-train docs by OL Root
        SELECT olroot.Id AS OLRoot, rtt.Id AS ReadyToTrainId, (SELECT COUNT(*) FROM [TotalAgility_Documents].[dbo].DocumentData AS d WHERE d.ParentId=rtt.InternalId) AS ReadyToTrainCount
        FROM [TotalAgility_Documents].[dbo].[FolderData] AS olroot  
        INNER JOIN [TotalAgility_Documents].[dbo].[FolderData] AS rtt ON rtt.ParentId=olroot.InternalId AND rtt.folderIndex=2 
        GROUP BY olroot.id, rtt.id,rtt.InternalId
        ) AS olmap ON olmap.OLRoot=olroot.id
        LEFT JOIN [TotalAgility].[dbo].[CAPTURE_ONLINE_LEARNING_MAPPING] AS olm 
        ON REPLACE(CONVERT(VARCHAR(36),olmap.ReadyToTrainid ,2),'-','')=CONVERT(VARCHAR(36), olm.ONLINE_LEARNING_FOLDER_ID,2)
        LEFT JOIN (
        SELECT co.ID, MAX(co.VERSION) AS MaxVersion 
        FROM [TotalAgility].[dbo].[CAPTURE_OBJECT] AS co
        GROUP BY co.ID
        ) AS m ON m.ID=olm.CAPTURE_OBJECT_ID
        LEFT JOIN [TotalAgility].[dbo].[CAPTURE_OBJECT] AS co ON (olm.CAPTURE_OBJECT_ID=co.ID AND co.VERSION=m.MaxVersion)
        WHERE olroot.FolderTypeXml like '%online%' --OL Root folder id
    END
RAISERROR('Progress 50pc completed', 0, 1) WITH NOWAIT


RAISERROR('This will check the TotalAgility DB index sizes and fragmentation-  note: Its quite normal for the TA db to have high fragmentation since its primary keys are GUIDs which will quickly get fragmented. This doesnt typically cause any problems.', 0, 1) WITH NOWAIT

use [TotalAgility]
SELECT [DatabaseName]
    ,[ObjectId]
    ,[ObjectName]
    ,[IndexId]
    ,[IndexDescription]
    ,CONVERT(DECIMAL(16, 1), (SUM([avg_record_size_in_bytes] * [record_count]) / (1024.0 * 1024))) AS [IndexSize(MB)]
    ,[lastupdated] AS [StatisticLastUpdated]
    ,[AvgFragmentationInPercent]
FROM (
    SELECT DISTINCT DB_Name(Database_id) AS 'DatabaseName'
        ,OBJECT_ID AS ObjectId
        ,Object_Name(Object_id) AS ObjectName
        ,Index_ID AS IndexId
        ,Index_Type_Desc AS IndexDescription
        ,avg_record_size_in_bytes
        ,record_count
        ,STATS_DATE(object_id, index_id) AS 'lastupdated'
        ,CONVERT([varchar](512), round(Avg_Fragmentation_In_Percent, 3)) AS 'AvgFragmentationInPercent'
    FROM sys.dm_db_index_physical_stats(db_id(), NULL, NULL, NULL, 'detailed')
    WHERE OBJECT_ID IS NOT NULL
        AND Avg_Fragmentation_In_Percent <> 0
    ) T
GROUP BY DatabaseName
    ,ObjectId
    ,ObjectName
    ,IndexId
    ,IndexDescription
    ,lastupdated
    ,AvgFragmentationInPercent

RAISERROR('This will check the TotalAgility Documents DB index sizes - in KTA 7.7 or below, its primary keys are GUIDs so will get high fragmentation quickly.  In the new data layer in 7.8 or above, this is not the case. ', 0, 1) WITH NOWAIT
use [TotalAgility_Documents]

SELECT [DatabaseName]
    ,[ObjectId]
    ,[ObjectName]
    ,[IndexId]
    ,[IndexDescription]
    ,CONVERT(DECIMAL(16, 1), (SUM([avg_record_size_in_bytes] * [record_count]) / (1024.0 * 1024))) AS [IndexSize(MB)]
    ,[lastupdated] AS [StatisticLastUpdated]
    ,[AvgFragmentationInPercent]
FROM (
    SELECT DISTINCT DB_Name(Database_id) AS 'DatabaseName'
        ,OBJECT_ID AS ObjectId
        ,Object_Name(Object_id) AS ObjectName
        ,Index_ID AS IndexId
        ,Index_Type_Desc AS IndexDescription
        ,avg_record_size_in_bytes
        ,record_count
        ,STATS_DATE(object_id, index_id) AS 'lastupdated'
        ,CONVERT([varchar](512), round(Avg_Fragmentation_In_Percent, 3)) AS 'AvgFragmentationInPercent'
    FROM sys.dm_db_index_physical_stats(db_id(), NULL, NULL, NULL, 'detailed')
    WHERE OBJECT_ID IS NOT NULL
        AND Avg_Fragmentation_In_Percent <> 0
    ) T
GROUP BY DatabaseName
    ,ObjectId
    ,ObjectName
    ,IndexId
    ,IndexDescription
    ,lastupdated
    ,AvgFragmentationInPercent

RAISERROR('Progress 100pc completed', 0, 1) WITH NOWAIT

SET NOCOUNT OFF  