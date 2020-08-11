using System;
using Serilog.Configuration;
using Serilog.Core;
using Serilog.Debugging;
using Serilog.Events;
using Serilog.Sinks.Oracle;

namespace Serilog
{
    /// <summary>
    /// Adds the WriteTo.Oracle() extension method to <see cref="LoggerConfiguration" />.
    /// </summary>
    public static class LoggerConfigurationOracleExtensions
    {
        /// <summary>
        /// Adds a sink that writes log events to a Oracle database.
        /// </summary>
        /// <param name="loggerConfiguration"></param>
        /// <param name="connectionString"></param>
        /// <param name="tableName"></param>
        /// <param name="restrictedToMinimumLevel"></param>
        /// <param name="storeTimestampInUtc"></param>
        /// <param name="batchSize"></param>
        /// <param name="levelSwitch"></param>
        /// <returns></returns>
        public static LoggerConfiguration Oracle(
            this LoggerSinkConfiguration loggerConfiguration,
            string connectionString,
            string tableName = "Logs",
            LogEventLevel restrictedToMinimumLevel = LevelAlias.Minimum,
            bool storeTimestampInUtc = false,
            uint batchSize = 100,
            LoggingLevelSwitch levelSwitch = null)
        {
            if(loggerConfiguration == null)
                throw new ArgumentNullException(nameof(loggerConfiguration));

            if (string.IsNullOrEmpty(connectionString))
                throw new ArgumentNullException(nameof(connectionString));

            if (batchSize < 1 || batchSize > 1000)
                throw new ArgumentOutOfRangeException("[batchSize] argument must be between 1 and 1000 inclusive");

            try
            {
                return loggerConfiguration.Sink(
                    new OracleSink(connectionString, tableName, storeTimestampInUtc, batchSize),
                    restrictedToMinimumLevel,
                    levelSwitch);
            }
            catch (Exception ex)
            {
                SelfLog.WriteLine(ex.Message);

                throw;
            }
        }
    }
}
