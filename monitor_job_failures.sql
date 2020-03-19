USE master
GO
DROP DATABASE sql_server_agent
GO
CREATE DATABASE sql_server_agent
GO
USE sql_server_agent
GO
CREATE SCHEMA maintenance
GO
IF NOT EXISTS (SELECT * FROM sys.tables WHERE tables.name = 'Job')
BEGIN
	CREATE TABLE maintenance.Job
	(	job_id INT NOT NULL IDENTITY(1,1) CONSTRAINT PK_job PRIMARY KEY CLUSTERED,
		job_id_guid UNIQUEIDENTIFIER NOT NULL,
		job_name NVARCHAR(128) NOT NULL,
		job_create_datetime DATETIME NOT NULL, 
		job_last_modified_datetime DATETIME NOT NULL,
		job_category_name VARCHAR(100) NOT NULL,
		job_alert_send_sms nvarchar(4000) NULL,
		job_alert_send_email nvarchar(4000) NULL,
		job_alert_ticket_register nvarchar(4000) NULL,
		is_enabled BIT NOT NULL,
		is_deleted BIT NOT NULL
	);
END
GO
IF NOT EXISTS (SELECT * FROM sys.tables WHERE tables.name = 'JobFailure')
BEGIN
	CREATE TABLE maintenance.JobFailure
	(	job_failure_id INT NOT NULL IDENTITY(1,1) CONSTRAINT PK_job_failure PRIMARY KEY CLUSTERED,
		job_id INT NOT NULL CONSTRAINT FK_job_failure_job FOREIGN KEY REFERENCES maintenance.job (job_id),
		instance_id INT NOT NULL,
		job_start_time DATETIME NOT NULL,
		job_failure_time DATETIME NOT NULL,
		job_failure_step_number SMALLINT NOT NULL,
		job_failure_step_name VARCHAR(250) NOT NULL,
		job_failure_message VARCHAR(MAX) NOT NULL,
		job_step_failure_message VARCHAR(MAX) NOT NULL,
		job_step_severity INT NOT NULL,
		job_step_message_id INT NOT NULL,
		retries_attempted INT NOT NULL,
		has_email_been_sent_to_operator BIT NOT NULL
	);

	CREATE NONCLUSTERED INDEX NCIX_JobFailure_job_id ON maintenance.JobFailure (job_id);
	CREATE NONCLUSTERED INDEX NCIX_Job_failure_instance_id ON maintenance.JobFailure (instance_id);
END
GO

CREATE PROCEDURE maintenance.dbasp_monitor_job_failure
	@minutes_to_monitor SMALLINT = 1440
AS
BEGIN
	SET NOCOUNT ON;
	-- First, collect list of SQL Server agent jobs and update ours as needed.
	-- Update our jobs data with any changes since the last update time.
	MERGE INTO maintenance.Job AS TARGET
		USING (SELECT
					myJob.job_id AS job_id_guid,
					myJob.name AS job_name,
					myJob.date_created AS job_create_datetime,
					myJob.date_modified AS job_last_modified_datetime,
					myJob.enabled AS is_enabled,
					0 AS is_deleted,
					ISNULL(myCategory.name, '') AS job_category_name
			   FROM msdb.dbo.sysjobs as myJob
			   LEFT JOIN msdb.dbo.syscategories as myCategory ON myCategory.category_id = myJob.category_id
			   ) AS SOURCE
		ON (SOURCE.job_id_guid = TARGET.job_id_guid)
		WHEN NOT MATCHED BY TARGET 
			THEN INSERT (job_id_guid, job_name, job_create_datetime, job_last_modified_datetime,job_category_name, is_enabled, is_deleted)
				 VALUES (SOURCE.job_id_guid, SOURCE.job_name, SOURCE.job_create_datetime, SOURCE.job_last_modified_datetime, SOURCE.job_category_name, SOURCE.is_enabled, SOURCE.is_deleted )
		WHEN MATCHED AND SOURCE.job_last_modified_datetime > TARGET.job_last_modified_datetime
			THEN UPDATE
				SET job_name = SOURCE.job_name,
					job_create_datetime = SOURCE.job_create_datetime,
					job_last_modified_datetime = SOURCE.job_last_modified_datetime,
					is_enabled = SOURCE.is_enabled,
					is_deleted = SOURCE.is_deleted,
					job_category_name = SOURCE.job_category_name;
	-- If a job was deleted, then mark it as no longer enabled.
	UPDATE myJob
		SET is_enabled = 0,
			is_deleted = 1
	FROM maintenance.Job as myJob
	LEFT JOIN msdb.dbo.sysjobs as sysJob ON sysJob.Job_Id = myJob.job_id_guid
	WHERE sysJob.Job_Id IS NULL;

	CREATE TABLE #Jobfailure
	(
		job_id_guid UNIQUEIDENTIFIER NOT NULL,
		job_start_time DATETIME NOT NULL,
		job_failure_time DATETIME NOT NULL,
		failure_message NVARCHAR(MAX) NOT NULL,
		instance_id INT NOT NULL,
		job_failure_step_number INT NOT NULL,
		job_step_severity INT NOT NULL,
		retries_attempted INT NOT NULL,
		step_name SYSNAME NOT NULL,
		sql_message_id INT NOT NULL
	);
	--Find all recent job failures and log them in the target log table.    
	;WITH myCTE 
	AS (
		SELECT
			job_id AS job_id_guid,
			CAST(FORMAT(DATEADD(S,(run_time/10000)*60*60 +((run_time - (run_time/10000) * 10000)/100) * 60 + (run_time - (run_time/100) * 100) ,CONVERT(DATETIME,RTRIM(run_date),113)),'yyyy-MM-dd HH:mm:ss') as datetime) as job_start_datetime ,
			(run_duration/10000)*60*60 +((run_duration - (run_duration/10000) * 10000)/100) * 60 + (run_duration - (run_duration/100) * 100) as duration_seconds ,
			run_status,
			CASE run_status
				WHEN 0 THEN 'Failure'
				WHEN 1 THEN 'Success'
				WHEN 2 THEN 'Retry'
				WHEN 3 THEN 'Canceled'
				ELSE 'Unknown'
			END AS job_status,
			message,
			instance_id,
			step_id AS job_failure_step_number,
			sql_severity AS job_step_severity,
			retries_attempted,
			step_name,
			sql_message_id
		FROM msdb.dbo.sysjobhistory as myJobHistory WITH (NOLOCK)
		WHERE run_status = 0)

	INSERT INTO #Jobfailure (job_id_guid,job_start_time,job_failure_time,failure_message,instance_id,job_failure_step_number,job_step_severity,retries_attempted,step_name,sql_message_id)
	SELECT
		job_id_guid,
		job_start_datetime AS job_start_time,
		DATEADD(SECOND, ISNULL(duration_seconds, 0), job_start_datetime) AS job_failure_time,
		ISNULL(message, '') AS failure_message,
		instance_id,
		job_failure_step_number,
		job_step_severity,
		retries_attempted,
		step_name,
		sql_message_id
	FROM myCTE 
	WHERE myCTE.job_start_datetime > DATEADD(MINUTE, -1 * @minutes_to_monitor, GETDATE());	
	-- Get jobs that failed due to failed steps.
	;WITH CTE_FAILURE_STEP 
	AS (
		SELECT ROW_NUMBER() OVER (PARTITION BY job_id_guid, job_failure_time ORDER BY job_failure_step_number DESC) AS RowNumber,
				job_id_guid,
				job_start_time,
				job_failure_time,
				failure_message,
				instance_id,
				job_failure_step_number,
				job_step_severity,
				retries_attempted,
				step_name,
				sql_message_id
		FROM #Jobfailure
		WHERE job_failure_step_number > 0
	)
	INSERT INTO maintenance.JobFailure (job_id, instance_id, job_start_time, job_failure_time, job_failure_step_number, job_failure_step_name, job_failure_message, job_step_failure_message, job_step_severity, job_step_message_id, retries_attempted, has_email_been_sent_to_operator)
	SELECT 
		myJob.job_id,
		myStep.instance_id,
		myJobFailure.job_start_time,
		myStep.job_failure_time,
		myStep.job_failure_step_number,
		myStep.step_name AS job_failure_step_name,
		myJobFailure.failure_message AS Job_failure_message,
		myStep.failure_message AS step_failure_message,
		myStep.job_step_severity,
		myStep.sql_message_id AS job_step_message_id,
		myStep.retries_attempted,
		0 AS has_email_been_sent_to_operator
	FROM #Jobfailure AS myJobFailure
	INNER JOIN maintenance.job AS myJob ON myJobFailure.job_id_guid = myJob.job_id_guid
	INNER JOIN CTE_FAILURE_STEP AS myStep ON myJobFailure.job_id_guid = myStep.job_id_guid AND myJobFailure.job_failure_time = myStep.job_failure_time
	WHERE myStep.RowNumber = 1 AND myJobFailure.job_failure_step_number = 0
		AND myStep.instance_id NOT IN (SELECT instance_id FROM maintenance.JobFailure);
	
	-- Get jobs that failed without any failed steps.
	INSERT INTO maintenance.JobFailure (job_id, instance_id, job_start_time, job_failure_time, job_failure_step_number, job_failure_step_name, job_failure_message, job_step_failure_message, job_step_severity, job_step_message_id, retries_attempted, has_email_been_sent_to_operator)
	SELECT 
		myJob.job_id,
		myJobFailure.instance_id,
		myJobFailure.job_start_time,
		myJobFailure.job_failure_time,
		job_failure_step_number AS job_failure_step_number,
		'' AS job_failure_step_name,
		myJobFailure.failure_message,
		'' AS job_step_failure_message,
		-1 AS job_step_severity,
		-1 AS job_step_message_id,
		0 AS retries_attempted,
		0 AS has_email_been_sent_to_operator
	FROM #Jobfailure AS myJobFailure
	LEFT JOIN maintenance.job AS myJob ON myJobFailure.job_id_guid = myJob.job_id_guid
	WHERE job_failure_step_number = 0 
		AND myJobFailure.instance_id NOT IN (SELECT instance_id FROM maintenance.JobFailure)
		AND NOT EXISTS (SELECT 1 FROM #Jobfailure AS myJobStep WHERE myJobStep.job_failure_step_number > 0 AND myJobFailure.job_id_guid = myJobStep.job_id_guid AND myJobFailure.job_failure_time = myJobStep.job_failure_time);
	-- Get job steps that failed, but for jobs that succeeded.
	WITH CTE_FAILURE_STEP 
	AS (
		SELECT
			ROW_NUMBER() OVER (PARTITION BY myFailure.job_id_guid, myFailure.job_failure_time ORDER BY myFailure.job_failure_step_number DESC) AS recent_step_rank,
			job_id_guid,
            job_start_time,
            job_failure_time,
            failure_message,
            instance_id,
            job_failure_step_number,
            job_step_severity,
            retries_attempted,
            step_name,
            sql_message_id
		FROM #Jobfailure AS myFailure
		WHERE job_failure_step_number >0
		)
	INSERT INTO maintenance.JobFailure (job_id, instance_id, job_start_time, job_failure_time, job_failure_step_number, job_failure_step_name, job_failure_message, job_step_failure_message, job_step_severity, job_step_message_id, retries_attempted, has_email_been_sent_to_operator)
	SELECT
		myJob.job_id,
		myStep.instance_id,
		myStep.job_start_time,
		myStep.job_failure_time,
		myStep.job_failure_step_number,
		myStep.step_name AS job_failure_step_name,
		'' AS job_failure_message,
		myStep.failure_message,
		myStep.job_step_severity,
		myStep.sql_message_id AS job_step_message_id,
		myStep.retries_attempted,
		0 AS has_email_been_sent_to_operator
	FROM CTE_FAILURE_STEP AS myStep
	INNER JOIN maintenance.job AS myJob ON myStep.job_id_guid = myJob.job_id_guid
	WHERE myStep.recent_step_rank = 1
	AND myStep.instance_id NOT IN (SELECT instance_id FROM maintenance.JobFailure)
	AND NOT EXISTS (SELECT 1 FROM #Jobfailure AS myJobFailure WHERE job_failure_step_number = 0 AND myJobFailure.job_id_guid = myStep.job_id_guid AND myJobFailure.job_failure_time = myStep.job_failure_time);
	
	DROP TABLE #Jobfailure
END
GO

CREATE PROCEDURE maintenance.dbasp_monitor_job_failure_send_alert
	@default_email_to_address VARCHAR(MAX) = NULL,
	@profile_name VARCHAR(MAX) = 'Default Public Profile'
AS
BEGIN
	SET NOCOUNT ON;
	DECLARE @myCursor CURSOR;
	DECLARE @email_subject VARCHAR(MAX);
	DECLARE @email_body VARCHAR(MAX);
	DECLARE @job_failure_count INT;

	DECLARE @myJobId INT;
	DECLARE @myJobName NVARCHAR(100);
	DECLARE @myJobAlertSendEmail VARCHAR(MAX);

	SELECT @job_failure_count = COUNT(*) FROM maintenance.JobFailure AS myJobFailure WHERE myJobFailure.has_email_been_sent_to_operator = 0;

	-- Send an email to an operator if any new errors are found.
	IF @job_failure_count > 0
	BEGIN
		SET @myCursor = CURSOR FOR 
						SELECT DISTINCT
							   myjob.job_id,
							   myjob.job_name,
							   myjob.job_alert_send_email
						FROM maintenance.JobFailure AS myJobFailure
						INNER JOIN maintenance.Job AS myjob ON myjob.job_id = myJobFailure.job_id 
						WHERE myJobFailure.has_email_been_sent_to_operator = 0;
		OPEN @myCursor
		FETCH NEXT FROM @myCursor INTO @myJobId, @myJobName, @myJobAlertSendEmail
		WHILE @@FETCH_STATUS=0
		BEGIN
			SET @email_subject = 'Failed Job Alert: ' + ISNULL(@@SERVERNAME, CAST(SERVERPROPERTY('ServerName') AS VARCHAR(MAX)));
			SET @email_body = 'At least one failure has occurred on ' + ISNULL(@@SERVERNAME, CAST(SERVERPROPERTY('ServerName') AS VARCHAR(MAX))) 
			SET @email_body = @email_body + ':
				<html><body><table border=1>
				<tr>
					<th colspan="6" bgcolor="#F29C89" align="left">Total Failed Jobs: ' + CAST(@job_failure_count AS VARCHAR(MAX)) + '</th>
				</tr>
				<tr>
					<th bgcolor="#F29C89">Job Name</th>
					<th bgcolor="#F29C89">Server Job Start Time</th>
					<th bgcolor="#F29C89">Server Job Failure Time</th>
					<th bgcolor="#F29C89">Failure Step Name</th>
					<th bgcolor="#F29C89">Job Failure Message</th>
					<th bgcolor="#F29C89">Job Step Failure Message</th>
				</tr>';
			SELECT @email_body = @email_body + 
								CAST((
									   SELECT CAST(@myJobName AS VARCHAR(MAX)) AS 'td', '',
									   		 CAST(myJobFailure.job_start_time AS VARCHAR(MAX)) AS 'td', '',
									   		 CAST(myJobFailure.job_failure_time AS VARCHAR(MAX)) AS 'td', '',
									   		 myJobFailure.job_failure_step_name AS 'td', '',
									   		 myJobFailure.job_failure_message AS 'td', '',
									   		 myJobFailure.job_step_failure_message AS 'td'
									   FROM maintenance.JobFailure AS myJobFailure
									   WHERE myJobFailure.job_id = @myJobId AND myJobFailure.has_email_been_sent_to_operator = 0
									   ORDER BY myJobFailure.job_failure_time ASC
									   FOR XML PATH('tr'), ELEMENTS
									 ) AS VARCHAR(MAX));

			SELECT @email_body = @email_body + '</table></body></html>';
			SELECT @email_body = REPLACE(@email_body, '<td>', '<td valign="top">');
			DECLARE @email_to_address VARCHAR(MAX) 
			SET @email_to_address = ISNULL(@myJobAlertSendEmail , @default_email_to_address)
			IF (@email_to_address IS NOT NULL)
				EXEC msdb.dbo.sp_send_dbmail
					@profile_name = @profile_name,
					@recipients = @email_to_address,
					@subject = @email_subject,
					@body_format = 'html',
					@body = @email_body;

			UPDATE myJobFailure
				SET has_email_been_sent_to_operator = 1
			FROM maintenance.JobFailure AS myJobFailure
			WHERE myJobFailure.job_id = @myJobId AND  myJobFailure.has_email_been_sent_to_operator = 0;
			FETCH NEXT FROM @myCursor INTO @myJobId, @myJobName, @myJobAlertSendEmail
		END
		CLOSE @myCursor;
		DEALLOCATE @myCursor;
	END
END
GO

CREATE VIEW maintenance.VIW_JobFailure
AS
	SELECT ROW_NUMBER()OVER(ORDER BY myJobFailure.job_start_time DESC,myJobFailure.job_failure_time DESC) AS RowNumber,myJob.job_name,myJob.job_category_name,myJob.is_enabled,myJob.is_deleted,
		   myJobFailure.job_failure_step_number,myJobFailure.job_failure_step_name,myJobFailure.job_start_time,myJobFailure.job_failure_time,
		   myJobFailure.job_failure_message,myJobFailure.job_step_failure_message,myJobFailure.retries_attempted,myJobFailure.has_email_been_sent_to_operator
	FROM maintenance.Job as myJob
	INNER JOIN maintenance.JobFailure as myJobFailure on myJob.job_id = myJobFailure.job_id
GO

EXECUTE maintenance.dbasp_monitor_job_failure 1440
GO
EXECUTE maintenance.dbasp_monitor_job_failure_send_alert 'z.saffarpour@gmail.com', 'Default Public Profile'
GO

SELECT * FROM maintenance.VIW_JobFailure
