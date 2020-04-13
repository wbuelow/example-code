

/*

Parameter-driven merge proc, utilizing etl.config table.
Code is heavily commented to show the building the dynamic sql

*/

CREATE   PROCEDURE [etl].[stp_mergeStageToTarget]
@DataFactoryName			NVARCHAR (100)		,
@PipelineName				NVARCHAR (100)		,
@PipelineRunId				UNIQUEIDENTIFIER	,
@PipelineTriggerName		NVARCHAR (100)		,
@PipelineTriggerType		NVARCHAR (100)		,
@SourceName					NVARCHAR (100)		

AS
BEGIN

	SET NOCOUNT ON;
	
	-----------------------Begin Chapter 1 - Preliminaries---------------------------------

		/*Comment in for debug*/

		--declare @SourceName nvarchar(100) = 'Polaris_CustomerHub.stg.RideCommandUserProfile'

		/* declare parameters */
		DECLARE @ErrorNumber INT, --for logging an error number, if we throw one
				@ErrorMessage NVARCHAR(4000),--for logging an error message, if we throw an error
				@InsertedRowCount INT, --for auditing rows inserted
				@UpdatedRowCount INT,--for auditing rows updated
				@StartDate DATETIME2 = SYSDATETIME(), --for logging processing time
				@EndDate DATETIME2, --for logging processing time
				@ProcessSeconds FLOAT, --for logging processing time
				@ProcessUserKey INT,
				@RemediationKey INT,
				@SourceEffectiveDateColumn NVARCHAR(100),
				@DestinationEffectiveDateColumn NVARCHAR(100),

				@SourceQuery NVARCHAR(MAX), --variable to hold the source part of the merge statement
				@MergeColumns NVARCHAR(MAX), 
				@KeyColumns NVARCHAR(100),
				@MergeColumnsNoKeys NVARCHAR(MAX),

				@OnStatement NVARCHAR(MAX), --variable to hold the part of the merge that describes on which fields we're merging
				
				@SourceColumnsStatement NVARCHAR(MAX),
				@TargetColumnsStatement NVARCHAR(MAX),
				@SourceColumnsStatementNoKeys NVARCHAR(MAX),
				@TargetColumnsStatementNoKeys NVARCHAR(MAX),

				@MergeMatchStatement NVARCHAR(MAX), -- variable to hold the BinaryChecksum on source and target
				@ValuesStatement NVARCHAR(MAX), -- varaible to hold the insert statement when not matched
				@UpdateStatement NVARCHAR(MAX), -- varaible to hold the update statement when matched
				@SQL NVARCHAR(MAX), --variable to hold the merge statement we'll execute
				
				@msg NVARCHAR(2048), --hold the custom error message we use in case of null sql
				
				@TableName NVARCHAR(100),
				@DestinationName NVARCHAR(100)

		SELECT @TableName = DestinationTable
				,@MergeColumns = R.MergeColumns + ',' + R.KeyColumns --the key column(s) should not be included in etl.config.MergeColumns
				,@KeyColumns = R.KeyColumns
				,@MergeColumnsNoKeys = R.MergeColumns
				,@SourceEffectiveDateColumn = R.SourceEffectiveDateColumn
				,@DestinationEffectiveDateColumn = R.DestinationEffectiveDateColumn
				,@DestinationName = DestinationName
				,@SourceQuery = SourceQuery
				,@ProcessUserKey = ProcessUserKey
			FROM etl.config R
			WHERE SourceName = @SourceName

		---------------------------------End Chapter 1 - Preliminaries---------------------------------

		---------------------------------Begin Chapter 2 - Assign string parameters to build dynamic SQL---------------------------------
		
		---------------------------------Begin Helper Parameters - @ColumnNames, @SourceColumns, @TargetColumns, @ColumnNamesKeysExcluded---------------------------------
		
		/* These parameters holds the column names for merge. We'll use most of them more than once in the dynamic sql, as well as to help populate other variables */

		SET @SourceColumnsStatement = 'Source.' + REPLACE(@MergeColumns, ',' , ',Source.')
		SET @TargetColumnsStatement = 'Target.' + REPLACE(@MergeColumns, ',' , ',Target.')

		SET @SourceColumnsStatementNoKeys = 'Source.' + REPLACE(@MergeColumnsNoKeys, ',' , ',Source.')
		SET @TargetColumnsStatementNoKeys = 'Target.' + REPLACE(@MergeColumnsNoKeys, ',' , ',Target.')

		---------------------------------End Helper Parameters - @ColumnNames, @SourceColumns, @TargetColumns, @ColumnNamesKeysExcluded---------------------------------
		
		---------------------------------Begin Parameter - @OnStatement---------------------------------
		/*
		This parameter holds the statement that defines on which columns we're merging. It uses the @keyColumnNames parameter passed to the proc,
		and handles cases in which there are multiple key columns defined.

		@OnStatement contains a string like:
	
		Target.SomeKeyColumn = Stage.SomeKeyColumn 
	
		After this, our merge statement will look something like:

		MERGE dbo.tableName AS TARGET
		USING (
		SELECT 
				CASE
					WHEN createdby IN ( '', '\N' ) THEN NULL 
					ELSE Cast(createdby AS UNIQUEIDENTIFIER) 
				END AS CreatedBy, 
				CASE 
					WHEN createdon IN ( '', '\N' ) THEN NULL 
					ELSE Cast(LEFT(createdon, Len(createdon) - 6) AS DATETIME) 
				END AS CreatedOn, 
				Cast(processuserkey AS INT) AS ProcessUserKey,
				...(etc)
			FROM stg.tableName
		) AS SOURCE ([CreatedBy],[CreatedOn],[ProcessUserKey],...(etc))
		ON (Target.SomeKeyColumn = Stage.SomeKeyColumn)

		*/

		DECLARE @KeyColumnsTable TABLE (Id INT IDENTITY(1,1), KeyColumn NVARCHAR(100)) --We'll use this to exclude the key columns from some parts of the merge (keep going; it'll make more sense later)
		INSERT @KeyColumnsTable (KeyColumn)
		SELECT [Value]
			FROM string_split(@KeyColumns, ',')
		
		SET @OnStatement = (
		SELECT STUFF(
			(SELECT ' AND ' + OnStatement
					FROM (
						SELECT 'Target.' + KeyColumn + ' = Source.' + KeyColumn AS OnStatement
							FROM @KeyColumnsTable
							) AS D
				FOR XML PATH (''))
			, 1, 5, '')
		)

		---------------------------------End Parameter - @OnStatement---------------------------------

		---------------------------------Begin Parameter - @MergeMatchStatement---------------------------------
		/*
		This parameter holds the statement that performs a binary checksum on the columns in the @ColumnNames variable

		@MergeMatchStatement contains a string like:
	
		BINARY_CHECKSUM( Source.[SomeKeyColumn]
						,Source.[CreatedBy]
						,Source.[CreatedOn]
						,Source.SomeBusinessColumn
						,...(etc)
						) <>
			BINARY_CHECKSUM( Target.[SomeKeyColumn]
							,Target.[CreatedBy]
							,Target.[CreatedOn]
							,Target.SomeBusinessColumn
							,...(etc)
							)
	
		After this, our merge statement will look something like:

		MERGE dbo.tableName AS TARGET
		USING (
		SELECT 
				CASE
					WHEN createdby IN ( '', '\N' ) THEN NULL 
					ELSE Cast(createdby AS UNIQUEIDENTIFIER) 
				END AS CreatedBy, 
				CASE 
					WHEN createdon IN ( '', '\N' ) THEN NULL 
					ELSE Cast(LEFT(createdon, Len(createdon) - 6) AS DATETIME) 
				END AS CreatedOn, 
				Cast(processuserkey AS INT) AS ProcessUserKey,
				...(etc)
			FROM stg.tableName
		) AS SOURCE ([CreatedBy],[CreatedOn],[ProcessUserKey],...(etc))
		ON (Target.SomeKeyColumn = Stage.SomeKeyColumn)
		WHEN MATCHED AND
		BINARY_CHECKSUM( Source.[CreatedBy]
						,Source.[CreatedOn]
						,Source.[ModifiedBy]
						,Source.[ModifiedOn]
						,Source.[SomeBusinessColumn]
						,...(etc)
						) <>
			BINARY_CHECKSUM( Target.[CreatedBy]
							,Target.[CreatedOn]
							,Target.[ModifiedBy]
							,Target.[ModifiedOn]
							,Target.[SomeBusinessColumn]
							,...(etc)
							)
		*/

		SET @MergeMatchStatement = 'BINARY_CHECKSUM(' + @SourceColumnsStatementNoKeys + ') <> BINARY_CHECKSUM(' + @TargetColumnsStatementNoKeys + ')'
	
		---------------------------------End Parameter - @MergeMatchStatement---------------------------------

		---------------------------------Begin Parameter - @UpdateStatement---------------------------------

		/*
		This parameter holds the update statement that we perform if we match on the key columns but there are differences between the stage and target data for other columns

		@UpdateStatement contains a string like:
	
		[CreatedBy] = Source.[CreatedBy],
		[CreatedOn] = Source.[CreatedOn],
		[ModifiedBy] = Source.[ModifiedBy],
		[ModifiedOn] = Source.[ModifiedOn],
		[SomeBusinessColumn] = Source.[SomeBusinessColumn],
		[ModifiedOn] = SYSDATETIME(),
		...(etc)
	
		After this, our merge statement will look something like:

		MERGE dbo.tableName AS TARGET
		USING (
		SELECT 
				CASE
					WHEN createdby IN ( '', '\N' ) THEN NULL 
					ELSE Cast(createdby AS UNIQUEIDENTIFIER) 
				END AS CreatedBy, 
				CASE 
					WHEN createdon IN ( '', '\N' ) THEN NULL 
					ELSE Cast(LEFT(createdon, Len(createdon) - 6) AS DATETIME) 
				END AS CreatedOn, 
				Cast(processuserkey AS INT) AS ProcessUserKey,
				...(etc)
			FROM stg.tableName
		) AS SOURCE ([CreatedBy],[CreatedOn],[ProcessUserKey],...(etc))
		ON (Target.SomeKeyColumn = Stage.SomeKeyColumn)
		WHEN MATCHED AND
		BINARY_CHECKSUM( Source.[CreatedBy]
						,Source.[CreatedOn]
						,Source.[ModifiedBy]
						,Source.[ModifiedOn]
						,Source.[SomeBusinessColumn]
						,...(etc)
						) <>
			BINARY_CHECKSUM( Target.[CreatedBy]
							,Target.[CreatedOn]
							,Target.[ModifiedBy]
							,Target.[ModifiedOn]
							,Target.[SomeBusinessColumn]
							,...(etc)
							)
		THEN UPDATE SET
		[CreatedBy] = Source.[CreatedBy],
		[CreatedOn] = Source.[CreatedOn],
		[ModifiedBy] = Source.[ModifiedBy],
		[ModifiedOn] = Source.[ModifiedOn],
		[ModifiedOn] = SYSDATETIME()
	
		*/

		DECLARE @MergeColumnsNoKeysTable TABLE (Id INT IDENTITY(1,1), Col NVARCHAR(100)) --We'll use this to exclude the key columns from some parts of the merge (keep going; it'll make more sense later)
		INSERT @MergeColumnsNoKeysTable (Col)
		SELECT [Value]
			FROM string_split(@MergeColumnsNoKeys, ',')

		SET @UpdateStatement = (
		SELECT STUFF(
			(SELECT ',' + UpdateStatement
					FROM (
						SELECT Col + ' = Source.' + Col AS UpdateStatement
							FROM @MergeColumnsNoKeysTable
							) AS D
				FOR XML PATH (''))
			, 1, 1, '')
		) + ',[ModifiedOn] = SYSDATETIME()'

		/* 
	
		---------------------------------End Parameter - @UpdateStatement---------------------------------

		---------------------------------End Chapter 2 - Assign string parameters to build dynamic SQL---------------------------------

		---------------------------------Begin Chapter 3 - Build and Execute Dynamic SQL, handle errors---------------------------------
	
		At this point, with the help of some parameters we've already defined, our merge statement will look something like this:

		MERGE dbo.tableName AS TARGET
		USING (
		SELECT 
				CASE
					WHEN createdby IN ( '', '\N' ) THEN NULL 
					ELSE Cast(createdby AS UNIQUEIDENTIFIER) 
				END AS CreatedBy, 
				CASE 
					WHEN createdon IN ( '', '\N' ) THEN NULL 
					ELSE Cast(LEFT(createdon, Len(createdon) - 6) AS DATETIME) 
				END AS CreatedOn, 
				Cast(processuserkey AS INT) AS ProcessUserKey,
				...(etc)
			FROM stg.tableName
		) AS SOURCE ([CreatedBy],[CreatedOn],[ProcessUserKey],...(etc))
		ON (Target.SomeKeyColumn = Stage.SomeKeyColumn)
		WHEN MATCHED AND
		BINARY_CHECKSUM( Source.[CreatedBy]
						,Source.[CreatedOn]
						,Source.[ModifiedBy]
						,Source.[ModifiedOn]
						,Source.[SomeBusinessColumn]
						,...(etc)
						) <>
			BINARY_CHECKSUM( Target.[CreatedBy]
							,Target.[CreatedOn]
							,Target.[ModifiedBy]
							,Target.[ModifiedOn]
							,Target.[SomeBusinessColumn]
							,...(etc)
							)
		THEN UPDATE SET
			[CreatedBy] = Source.[CreatedBy],
			[CreatedOn] = Source.[CreatedOn],
			[ModifiedBy] = Source.[ModifiedBy],
			[ModifiedOn] = Source.[ModifiedOn],
			[SomeBusinessColumn] = Source.[SomeBusinessColumn],
			,...(etc)
			[ModifiedOn] = SYSDATETIME()
		WHEN NOT MATCHED THEN
			INSERT (
			[CreatedBy]
			[CreatedOn]
			[ModifiedBy]
			[ModifiedOn]
			[SomeBusinessColumn]
			)
			VALUES
			(
			Source.[CreatedBy]
			,Source.[CreatedOn]
			,Source.[ModifiedBy]
			,Source.[ModifiedOn]
			,Source.[SomeBusinessColumn]
			)
		OUTPUT $action
		INTO ##Output;
		
		Let's put it together : 
		*/

		SET @SQL =
			'DROP TABLE IF EXISTS ##Output' + @TableName +
			' CREATE TABLE ##Output' + @TableName + '  (Change VARCHAR(20))' + 
			' MERGE dbo.' + @TableName + ' AS TARGET' + 
			' USING (' + @SourceQuery + 
			' ) AS SOURCE ON (' + @OnStatement + ')' + 
			' WHEN MATCHED AND ' + @MergeMatchStatement +
			' THEN UPDATE SET ' + @UpdateStatement + 
			' WHEN NOT MATCHED THEN' + 
			' INSERT (' + @MergeColumns + ')' +
			' VALUES (' + @SourceColumnsStatement + ')' + 
			' OUTPUT $action INTO ##Output' + @TableName + ';'

		/*Comment in for debug*/
		--select @sql

		/* if the sql string is null, something went wrong and we need to throw an error, otherwise it will go undetected */
		IF @SQL IS NULL 
			BEGIN 
				SET @msg = FORMATMESSAGE(N'NULL String for EXECUTE sp_executeSQL @SQL, @TableName = (%s)', @TableName);
				THROW 60000, @msg, 1
			END

		EXEC sp_executeSQL @SQL
		
		/* Table to hold the output of the merge output */
		DECLARE @RowCountTable TABLE (RowsAffected INT, Change NVARCHAR(20))
		INSERT INTO @RowCountTable
		EXEC(' SELECT COUNT(*), Change FROM ##Output' + @TableName + ' GROUP BY Change')
		SET @InsertedRowCount = (SELECT COUNT(*) FROM @RowCountTable WHERE Change = 'INSERT')
		SET @UpdatedRowCount = (SELECT COUNT(*) FROM @RowCountTable WHERE Change = 'UPDATE')

		SET @SQL = ' UPDATE etl.config
		SET DestinationMaxEffectiveDate = (SELECT MAX(' + @DestinationEffectiveDateColumn + ') FROM ' + @DestinationName + ')
			,ModifiedOn = SYSDATETIME()
		WHERE SourceName = ''' + @SourceName + ''''
		EXEC sp_executeSQL @SQL
		--uncomment for debug
		--select @sql
		
		SET @SQL = ' UPDATE etl.config
		SET SourceMaxEffectiveDate = (
		SELECT MAX(
			CASE 
				WHEN RIGHT(' + @SourceEffectiveDateColumn + ', 1) = ''M'' 
				THEN ' + @SourceEffectiveDateColumn +  
				' ELSE CAST(LEFT(' + @SourceEffectiveDateColumn + ', 16) AS DATETIME)' + --time truncated to minutes;
			'END
			) FROM ' + @SourceName + ')
			,ModifiedOn = SYSDATETIME()
		WHERE SourceName = ''' + @SourceName + ''''
		EXEC sp_executeSQL @SQL
		--uncomment for debug
		--select @sql

		SET @EndDate = SYSDATETIME()
		SET @ProcessSeconds = DATEDIFF(MILLISECOND, @StartDate, @EndDate)/1000.0

		EXECUTE [etl].[stp_insertProcessLog] @ProcessUserKey, 
						@DataFactoryName, 
						@PipelineName, 
						@PipelineRunId,
						@PipelineTriggerName,
						@PipelineTriggerType,
						'Stored Procedure',
						'SP-mergeStageToTarget',
						'Azure SQL Database Table',
						@SourceName,
						'Azure SQL Database Table',
						@DestinationName, 
						@InsertedRowCount, 
						@UpdatedRowCount, 
						0, 
						@ProcessSeconds, 
						NULL, 
						NULL

END
GO