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
where section_id = 2
ORDER BY 2
    ,3;"
$sqlFileDirectory = "C:\Users\student\Documents\polarexpress\version 1\Entity"

$connectionString = "Server=$serverName;Database=$databaseName;Integrated Security=True;"
$connection = New-Object System.Data.SqlClient.SqlConnection
$connection.ConnectionString = $connectionString

$connection.Open()

$command = $connection.CreateCommand()
$command.CommandText = $query

$result = $command.ExecuteReader()

$mainTable = New-Object System.Data.DataTable
$mainTable.Load($result)

$section1FilePath = Join-Path $sqlFileDirectory "METADATA.ETLEntityID.sql"

# Empty the files if they already exist
if (Test-Path $section1FilePath) {
    Clear-Content $section1FilePath
}

# Get variables A, B, C, and D
$section1Rows = $mainTable.Select("section_id = 2")
$A = $section1Rows[0]["Insert Statement"].Split('(')[1].Split(',')[0]
$B = $section1Rows[1]["Insert Statement"].Split('(')[1].Split(',')[0]
$C = $section1Rows[2]["Insert Statement"].Split('(')[1].Split(',')[0]
$D = $section1Rows[3]["Insert Statement"].Split('(')[1].Split(',')[0]

# Generate the header for the section 2 SQL file
$headerSection1 = "
USE ROLE OWNER_(environment);
USE DATABASE ENT_(environment);
USE SCHEMA TEST;
BEGIN TRANSACTION;
DELETE FROM ETLENTITY WHERE ETLENTITY ID IN ($($A),$($B),$($C),$($D));
INSERT INTO ETLENTITY(ENTITYID,ACCREF,SYSTEM,DBNAME,SCHEMA,TABLENAME,CREATEDON) VALUES
"

Add-Content -Path $section1FilePath -Value $headerSection1

$lastRowInSection1 = $mainTable.Select("section_id = 2")[-1]

foreach ($row in $mainTable.Rows) {
    $insertStatement = $row["Insert Statement"]

    if ($row -eq $lastRowInSection1 ) {
        $insertStatement = $insertStatement.TrimEnd(',') + ';'
    }

    Add-Content -Path $section1FilePath -Value $insertStatement
}
# Add "COMMIT;" to the end of the section 1 SQL file
Add-Content -Path $section1FilePath -Value "COMMIT;"

$connection.Close()

Write-Host "ENTITY data exported to $section1FilePath"
