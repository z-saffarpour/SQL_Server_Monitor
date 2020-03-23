--========================================================================
	DECLARE @FromDisk VARCHAR(max)
	DECLARE @DatabaseName VARCHAR(max)
	DECLARE @moveFileComand VARCHAR (4000),@InstanceDefaultDataPath VARCHAR(max),@InstanceDefaultLogPath VARCHAR(max), @myFileListOnly NVARCHAR(max),@myHeaderOnly NVARCHAR(max)

	SET @FromDisk = N'U:\Databases\Backup01\1398_11\03\LOG_DBA_1398_11_03_on_10_01_49_1of4.trn' --

	SET @InstanceDefaultDataPath = CAST(ServerProperty(N'InstanceDefaultDataPath') AS VARCHAR(4000))
	SET @InstanceDefaultLogPath = CAST(ServerProperty(N'InstanceDefaultLogPath') AS VARCHAR(4000))

	DECLARE @FileListOnlyTable TABLE 
	(
		[LogicalName]           NVARCHAR(128),
		[PhysicalName]          NVARCHAR(260),
		[Type]                  CHAR(1),
		[FileGroupName]         NVARCHAR(128),
		[Size]                  NUMERIC(20,0),
		[MaxSize]               NUMERIC(20,0),
		[FileID]                BIGINT,
		[CreateLSN]             NUMERIC(25,0),
		[DropLSN]               NUMERIC(25,0),
		[UniqueID]              UNIQUEIDENTIFIER,
		[ReadOnlyLSN]           NUMERIC(25,0),
		[ReadWriteLSN]          NUMERIC(25,0),
		[BackupSizeInBytes]     BIGINT,
		[SourceBlockSize]       INT,
		[FileGroupID]           INT,
		[LogGroupGUID]          UNIQUEIDENTIFIER,
		[DifferentialBaseLSN]   NUMERIC(25,0),
		[DifferentialBaseGUID]  UNIQUEIDENTIFIER,
		[IsReadOnly]            BIT,
		[IsPresent]             BIT,
		[TDEThumbprint]         VARBINARY(32) ,
		[SnapshotUrl]           VARCHAR(32) 
	)

	DECLARE @myRestoreHeaderOnlyTable TABLE 
	(
		[BackupName] nvarchar(128),
		[BackupDescription] nvarchar(255),
		[BackupType] smallint,
		[ExpirationDate] datetime,
		[Compressed] BIT,
		[Position] smallint,
		[DeviceType] tinyint,
		[UserName] nvarchar(128),
		[ServerName] varchar(50),
		[DatabaseName] varchar(50),
		[DatabaseVersion] int,
		[DatabaseCreationDate] datetime,
		[BackupSize] numeric(20,0),
		[FirstLSN] numeric(25,0),
		[LastLSN] numeric(25,0),
		[CheckpointLSN] numeric(25,0),
		[DatabaseBackupLSN] numeric(25,0),
		[BackupStartDate] datetime,
		[BackupFinishDate] datetime,
		[SortOrder] smallint,
		[CodePage] smallint,
		[UnicodeLocaleId] int,
		[UnicodeComparisonStyle] int,
		[CompatibilityLevel] tinyint,
		[SoftwareVendorId] int,
		[SoftwareVersionMajor] int,
		[SoftwareVersionMinor] int,
		[SoftwareVersionBuild] int,
		[MachineName] varchar(50),
		[Flags] int,
		[BindingID] uniqueidentifier,
		[RecoveryForkID] uniqueidentifier,
		[Collation] nvarchar(128),
		[FamilyGUID] uniqueidentifier,
		[HasBulkLoggedData] bit,
		[IsSnapshot] bit,
		[IsReadOnly] bit,
		[IsSingleUser] bit,
		[HasBackupChecksums] bit,
		[IsDamaged] bit,
		[BeginsLogChain] bit,
		[HasIncompleteMetaData] bit,
		[IsForceOffline] bit,
		[IsCopyOnly] bit,
		[FirstRecoveryForkID] uniqueidentifier,
		[ForkPointLSN] numeric(25,0),
		[RecoveryModel] nvarchar(60),
		[DifferentialBaseLSN] numeric(25,0),
		[DifferentialBaseGUID] uniqueidentifier,
		[BackupTypeDescription] nvarchar(60),
		[BackupSetGUID] uniqueidentifier,
		[CompressedBackupSize] bigint,
		[containment] tinyint,
		[KeyAlgorithm] nvarchar(32),
		[EncryptorThumbprint] varbinary(20),
		[EncryptorType] nvarchar(32)
	)
--========================================================================
	SET @myFileListOnly = 'RESTORE FILELISTONLY FROM DISK = N'''+ @FromDisk  +''''
	SET @myHeaderOnly = 'RESTORE HEADERONLY FROM DISK = N''' + @FromDisk + ''''
	SET @moveFileComand = CAST('' AS VARCHAR(4000))

	INSERT INTO @FileListOnlyTable 
	EXECUTE sys.sp_executesql @myFileListOnly

	INSERT INTO @myRestoreHeaderOnlyTable 
	EXECUTE sys.sp_executesql @myHeaderOnly

	SELECT @DatabaseName = DatabaseName
	FROM @myRestoreHeaderOnlyTable
--========================================================================
	IF(@DatabaseName IS NOT NULL)
	BEGIN
		SET @moveFileComand = 'RESTORE DATABASE ' + @DatabaseName +' FROM DISK = N''' + @FromDisk  +''' WITH REPLACE,STATS=2,RECOVERY'
		SELECT @moveFileComand = @moveFileComand + CASE WHEN @moveFileComand != '' THEN ',' ELSE '' END + 
								' MOVE N''' + LogicalName + ''' TO N''' + CASE WHEN Type = 'L' THEN @InstanceDefaultLogPath ELSE @InstanceDefaultDataPath END  + LogicalName + 
								CASE WHEN Type = 'L' THEN '.ldf' WHEN Type = 'D' AND FileID = 1 THEN '.mdf' ELSE '.ndf 'END + ''''
		FROM @FileListOnlyTable
		SELECT @moveFileComand
	END