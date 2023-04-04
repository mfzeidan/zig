$serverName = "DESKTOP-2GGPM29\SQLEXPRESS"
$databaseName = "iar_dev"
$query = @"
DECLARE @ABC_ID AS INT = 10000 --this needs to be changed
DECLARE @ETL_EntityID AS INT = 10000 --this needs to be changed
DECLARE @Env_Name AS VARCHAR(100) = 'DEV' --this needs to be changed
DECLARE @Env_param_upper AS VARCHAR(100) = '${environment}'
DECLARE @Env_param_lower AS VARCHAR(100) = '${environment_lower}'
DECLARE @Table_Name AS VARCHAR(250) = 'pb_adjustment_type' --this needs to be changed
DECLARE @Segment AS VARCHAR(100) = 'GPE'
DECLARE @S3_BUCKET_param AS VARCHAR(100) ='${bucket_name}'
DECLARE @SSMS_Schema AS VARCHAR(100) = 'dbo'
DECLARE @DB_PART_Name_upper AS VARCHAR(100)='IAR'
DECLARE @DB_PART_Name_lower AS VARCHAR(100)='iar'
DECLARE @Snowflake_schema AS VARCHAR(100) = 'IAR'
DECLARE @DB_Name AS VARCHAR(100) = 'IAR_' + @Env_Name
DECLARE @ETLAUDITID_1 AS VARCHAR(100) = @ETL_EntityID
DECLARE @ETLAUDITID_2 AS VARCHAR(100) = @ETL_EntityID + 1
DECLARE @ETLAUDITID_3 AS VARCHAR(100) = @ETL_EntityID + 2
DECLARE @ETLAUDITID_4 AS VARCHAR(100) = @ETL_EntityID + 3


SELECT *
FROM [dbo].[tfn_Snowflake_Generate_Insert_Statements](@ABC_ID, @ETL_EntityID, @Env_Name,@Env_param_upper,@Env_param_lower, @DB_Name,@DB_PART_Name_upper,@DB_PART_Name_lower,@Table_Name, @Segment,@S3_BUCKET_param, @SSMS_Schema, @Snowflake_schema, @ETLAUDITID_1, @ETLAUDITID_2, @ETLAUDITID_3, @ETLAUDITID_4)
where section_id in (3,4,5)
ORDER BY 2
    ,3;
"@
$sqlFileDirectory = "C:\Users\student\Documents\polarexpress\version 1\EntityAttribute"

$connectionString = "Server=$serverName;Database=$databaseName;Integrated Security=True;"
$connection = New-Object System.Data.SqlClient.SqlConnection
$connection.ConnectionString = $connectionString

$connection.Open()

$command = $connection.CreateCommand()
$command.CommandText = $query

$result = $command.ExecuteReader()

$mainTable = New-Object System.Data.DataTable
$mainTable.Load($result)

$outputFilePath = Join-Path $sqlFileDirectory "ETLEntityAttribute.sql"

# Empty the file if it already exists
if (Test-Path $outputFilePath) {
    Clear-Content $outputFilePath
}

$distinctIds = ($mainTable.Rows | ForEach-Object { $_["Insert Statement"].Split('(')[1].Split(',')[0] } | Select-Object -Unique) -join ","
$sqlFileContent = @"
USE ROLE OWNER_(environment);
USE DATABASE ENT_(environment);
USE SCHEMA TEST;
BEGIN TRANSACTION;
DELETE FROM ETLENTITYATTRIBUTE WHERE ETLENTITY ID IN ($distinctIds);
INSERT INTO ETLENTITYATTRIBUTE VALUES
"@

Add-Content -Path $outputFilePath -Value $sqlFileContent

$lastRow = $mainTable.Rows[-1]

$lastRowIndex = $mainTable.Rows.Count - 1

for ($i = 0; $i -lt $mainTable.Rows.Count; $i++) {
    $row = $mainTable.Rows[$i]
    $insertStatement = $row["Insert Statement"]

    if ($i -eq $lastRowIndex) {
        $insertStatement = $insertStatement.TrimEnd(',') + ';'
    }

    Add-Content -Path $outputFilePath -Value $insertStatement
}

Add-Content -Path $outputFilePath -Value "COMMIT;"

$connection.Close()

Write-Host "ETLEntityAttribute data exported to $outputFilePath"