$serverName = "DESKTOP-2GGPM29\SQLEXPRESS"
$databaseName = "iar_dev"
$query = "DECLARE @ABC_ID AS INT = 10000 --this needs to be changed
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
where section_id = 6
ORDER BY 2
    ,3;"
$sqlFileDirectory = "C:\Users\student\Documents\polarexpress\version 1\Parms"

$connectionString = "Server=$serverName;Database=$databaseName;Integrated Security=True;"
$connection = New-Object System.Data.SqlClient.SqlConnection
$connection.ConnectionString = $connectionString

$connection.Open()

$command = $connection.CreateCommand()
$command.CommandText = $query

$result = $command.ExecuteReader()

$mainTable = New-Object System.Data.DataTable
$mainTable.Load($result)

$section6FilePath = Join-Path $sqlFileDirectory "METADATA - ETLControlParameter.sql"

# Empty the file if it already exists
if (Test-Path $section6FilePath) {
    Clear-Content $section6FilePath
}

# Get distinct ParmGroupID values for section 6
$section6Rows = $mainTable.Select()
$distinctParmGroupIDs = $section6Rows | Select-Object -ExpandProperty 'Insert Statement' | ForEach-Object { [int](($_.Split('(')[1].Split(',')[0]) -replace '[^0-9]', '') } | Select-Object -Unique | Sort-Object


$X6 = $distinctParmGroupIDs[0]
$Y6 = $distinctParmGroupIDs[1]

# Generate the header for the section 6 SQL file
$headerSection6 = @"
USE ROLE OWNER_$($environment);
USE DATABASE ENT_$($environment);
USE SCHEMA TEST;
BEGIN TRANSACTION;
DELETE FROM CONTROLPARMS WHERE PARMGROUPID IN ($($X6),$($Y6));
INSERT INTO CONTROLPARMS (PARMGROUPID, PARMNAME, PARMVALUE,PARMDESCRIPTION) VALUES
"@

Add-Content -Path $section6FilePath -Value $headerSection6

$lastRowInSection6 = $mainTable.Select()[-1]

foreach ($row in $mainTable.Rows) {
    $insertStatement = $row["Insert Statement"]

    if ($row -eq $lastRowInSection6) {
        $insertStatement = $insertStatement.TrimEnd(',') + ';'
    }

    Add-Content -Path $section6FilePath -Value $insertStatement
}

# Add "COMMIT;" to the end of the section 6 SQL file
Add-Content -Path $section6FilePath -Value "COMMIT;"

$connection.Close()

Write-Host "Section 6 data exported to $section6FilePath"