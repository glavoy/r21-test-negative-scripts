INSERT INTO [dbo].[formchanges] (
	[tablename],
	[fieldname],
	[uniqueid],
	[oldvalue],
	[newvalue],
	[changed_at]
)
SELECT 
	[tablename],
	[fieldname],
	[uniqueid],
	NULLIF([oldvalue], ''),
	NULLIF([newvalue], ''),
	CAST(NULLIF(REPLACE([changed_at], 'T', ' '), '') AS datetime2(6))
FROM [dbo].[formchanges_staging]
