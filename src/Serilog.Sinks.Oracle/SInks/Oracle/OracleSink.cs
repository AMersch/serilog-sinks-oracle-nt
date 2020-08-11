namespace Serilog.Sinks.Oracle
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Text;
    using System.Threading.Tasks;

    using global::Oracle.ManagedDataAccess.Client;

    using Serilog.Core;
    using Serilog.Debugging;
    using Serilog.Events;
    using Serilog.Sinks.Batch;
    using Serilog.Sinks.Extensions;

    internal class OracleSink : BatchProvider, ILogEventSink
    {
        private readonly string _connectionString;
        private readonly bool _storeTimestampInUtc;
        private readonly string _tableName;

        public OracleSink(
            string connectionString,
            string tableName = "Logs",
            bool storeTimestampInUtc = false,
            uint batchSize = 100) : base((int)batchSize)
        {
            _connectionString = connectionString;
            _tableName = tableName;
            _storeTimestampInUtc = storeTimestampInUtc;

            var sqlConnection = GetSqlConnection();
            CreateTable(sqlConnection);
        }

        public void Emit(LogEvent logEvent)
        {
            PushEvent(logEvent);
        }

        private OracleConnection GetSqlConnection()
        {
            try
            {
                var conn = new OracleConnection(_connectionString);
                conn.Open();

                return conn;
            }
            catch (Exception ex)
            {
                SelfLog.WriteLine(ex.Message);

                return null;
            }
        }

        private OracleCommand GetInsertCommand(OracleConnection sqlConnection)
        {
            var sql = $@"
INSERT INTO  {_tableName} (
    TIMESTAMP
    , LOGLEVEL
    , MESSAGETEMPLATE
    , MESSAGE
    , EXCEPTION
    , PROPERTIES)
VALUES (
    :ts
    , :level
    , :template
    , :msg
    , :ex
    , :prop
)
";
            var cmd = sqlConnection.CreateCommand();
            cmd.CommandText = sql;

            cmd.Parameters.Add(new OracleParameter(":ts", OracleDbType.TimeStamp));
            cmd.Parameters.Add(new OracleParameter(":level", OracleDbType.NVarchar2));
            cmd.Parameters.Add(new OracleParameter(":template", OracleDbType.Clob));
            cmd.Parameters.Add(new OracleParameter(":msg", OracleDbType.Clob));
            cmd.Parameters.Add(new OracleParameter(":ex", OracleDbType.Clob));
            cmd.Parameters.Add(new OracleParameter(":prop", OracleDbType.Clob));

            return cmd;
        }

        private void CreateTable(OracleConnection sqlConnection)
        {
            try
            {
                var sql = $@"
declare
v_sql LONG;
begin

v_sql:='CREATE TABLE {_tableName}
  (
  ID INT NOT NULL ENABLE,
  TIMESTAMP TIMESTAMP NOT NULL,
  LOGLEVEL NVARCHAR2(128)  NULL,
  MESSAGETEMPLATE CLOB NULL,
  MESSAGE CLOB NULL,
  EXCEPTION CLOB NULL,
  PROPERTIES CLOB NULL
  );

  ALTER TABLE {_tableName} ADD CONSTRAINT PK_{_tableName} PRIMARY KEY (ID);

  CREATE SEQUENCE {_tableName}_seq START WITH 1 INCREMENT BY 1;

  CREATE TRIGGER 
	{_tableName}_trigger 
    BEFORE INSERT ON 
	    {_tableName} 
    REFERENCING 
	    NEW AS NEW 
	    OLD AS old 
    FOR EACH ROW 
    BEGIN 
	    IF :new.ID IS NULL THEN 
            SELECT {_tableName}_seq.NEXTVAL INTO :new.ID FROM dual;
        END IF;
    END;';

execute immediate v_sql;

EXCEPTION
    WHEN OTHERS THEN
      IF SQLCODE = -955 THEN
        NULL; -- suppresses ORA-00955 exception
      ELSE
         RAISE;
      END IF;
END; 
";
                var cmd = sqlConnection.CreateCommand();
                cmd.CommandText = sql;
                cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                SelfLog.WriteLine(ex.Message);
            }
        }

        protected override async Task<bool> WriteLogEventAsync(ICollection<LogEvent> logEventsBatch)
        {
            try
            {
                using (var sqlCon = GetSqlConnection())
                {
                    using (var tr = sqlCon.BeginTransaction())
                    {
                        var insertCommand = GetInsertCommand(sqlCon);
                        insertCommand.Transaction = tr;

                        foreach (var logEvent in logEventsBatch)
                        {
                            var logMessageString = new StringWriter(new StringBuilder());
                            logEvent.RenderMessage(logMessageString);

                            insertCommand.Parameters[":ts"].Value = _storeTimestampInUtc
                                ? logEvent.Timestamp.ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss.fffzzz")
                                : logEvent.Timestamp.ToString("yyyy-MM-dd HH:mm:ss.fffzzz");

                            insertCommand.Parameters[":level"].Value = logEvent.Level.ToString();
                            insertCommand.Parameters[":template"].Value = logEvent.MessageTemplate.ToString();
                            insertCommand.Parameters[":msg"].Value = logMessageString;
                            insertCommand.Parameters[":ex"].Value = logEvent.Exception?.ToString();
                            insertCommand.Parameters[":prop"].Value = logEvent.Properties.Count > 0
                                ? logEvent.Properties.Json()
                                : string.Empty;

                            await insertCommand.ExecuteNonQueryAsync().ConfigureAwait(false);
                        }

                        tr.Commit();

                        return true;
                    }
                }
            }
            catch (Exception ex)
            {
                SelfLog.WriteLine(ex.Message);

                return false;
            }
        }
    }
}
