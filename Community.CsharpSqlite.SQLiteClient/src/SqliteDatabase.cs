using System.Data;
using System.Data.Common;

namespace Community.CsharpSqlite.SQLiteClient
{
    public class SqliteDatabase
    {
        public string FileName { get; }

        public bool IsOpen => Connection?.State == ConnectionState.Open;



        private string ConnectionString { get; set; }
        private DbConnection Connection { get; set; }



        public SqliteDatabase()
        { }


        public SqliteDatabase(string fileName)
        {
            FileName = fileName;
            ConnectionString = $"Data Source=file://{fileName}";
        }



        /// <summary>
        /// Opens the database.
        /// </summary>
        public void Open()
        {
            Connection = SqliteClientFactory.Instance.CreateConnection();
            Connection.ConnectionString = ConnectionString;
            Connection.Open();
        }


        /// <summary>
        /// Closes the database.
        /// </summary>
        public void Close()
        {
            Connection.Close();
            Connection = null;
        }


        /// <summary>
        /// Execute an arbitrary sql command.
        /// </summary>
        public int ExecuteCommand(string sql)
        {
            var cmd = Connection.CreateCommand();
            cmd.CommandText = sql;
            return cmd.ExecuteNonQuery();
        }



        /// <summary>
        /// Execute sql query and return a DbDataReader with records to iterate over.
        /// </summary>
        public DbDataReader ExecuteReader(string sql)
        {
            var cmd = Connection.CreateCommand();
            cmd.CommandText = sql;
            return cmd.ExecuteReader();
        }



        /// <summary>
        /// Executes the query and returns the first column of the first row in the result
        //  set returned by the query. All other columns and rows are ignored.
        /// </summary>
        public object ExecuteScalar(string sql)
        {
            var cmd = Connection.CreateCommand();
            cmd.CommandText = sql;
            return cmd.ExecuteScalar();
        }

    }
}
