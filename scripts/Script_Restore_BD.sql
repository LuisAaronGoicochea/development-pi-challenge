USE master;
GO

-- Verifica si la base de datos existe y ciérrala si está abierta
IF DB_ID('Testing_ETL') IS NOT NULL
BEGIN
    ALTER DATABASE Testing_ETL SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE Testing_ETL;
END

-- Restaurar la base de datos desde el archivo .bak o crearla si no existe
RESTORE DATABASE Testing_ETL
FROM DISK = 'C:\Program Files\Microsoft SQL Server\MSSQL16.SQLEXPRESS\MSSQL\Backup\Testing_ETL.bak'
WITH MOVE 'Testing_ETL' TO 'C:\Program Files\Microsoft SQL Server\MSSQL16.SQLEXPRESS\MSSQL\Data\Testing_ETL.mdf',
MOVE 'Testing_ETL_log' TO 'C:\Program Files\Microsoft SQL Server\MSSQL16.SQLEXPRESS\MSSQL\Log\Testing_ETL_log.ldf',
REPLACE;