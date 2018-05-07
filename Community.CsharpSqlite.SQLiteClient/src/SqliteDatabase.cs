using System;
using System.Collections.Generic;
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




        public IEnumerable<Dictionary<string, object>> GetRecords(string sql)
        {
            var result = new List<Dictionary<string, object>>();
            using (var reader = ExecuteReader(sql))
            {
                while (reader.Read())
                {
                    var fieldCount = reader.FieldCount;
                    var rec = new Dictionary<string, object>();
                    for (int fieldNo = 0; fieldNo < fieldCount; fieldNo++)
                    {
                        var fieldName = reader.GetName(fieldNo);
                        var fieldValue = reader.GetValue(fieldNo);
                        rec[fieldName] = fieldValue;
                    }
                    result.Add(rec);
                }
            }
            return result;

        }


        public IEnumerable<string> ListTables()
        {
            var result = new List<string>();
            string sql = "SELECT name FROM SQLITE_MASTER where type='table'";
            using (var reader = ExecuteReader(sql))
            {
                while (reader.Read())
                {
                    var tableName = reader["Name"];
                    result.Add(tableName.ToString());
                }
            }
            return result;
        }


        /// <summary>
        /// Return the results of a sql query as a table
        /// </summary>
        public DataTable GetTable(string sql)
        {
            DataTable dt = new DataTable();
            using (var reader = ExecuteReader(sql))
                dt.Load(reader);
            return dt;
        }





        public void Insert(string tableName, Dictionary<string,object> rec)
        {
            if (rec.Count == 0)
                throw new Exception("Record contains no data to insert.");
            var columns = "";
            var values = "";
            foreach (var val in rec)
            {
                columns += $" {val.Key},";
                values += $" '{val.Value}',";
            }
            columns = columns.Substring(0, columns.Length - 1);
            values = values.Substring(0, values.Length - 1);
            ExecuteCommand($"insert into {tableName} ({columns}) values ({values});");
        }



        public void Update(string tableName, Dictionary<string, object> rec, string whereClause)
        {
            if (rec.Count == 0)
                throw new Exception("Record contains no data to update.");
            if (string.IsNullOrWhiteSpace(whereClause))
                throw new Exception("You must provide a 'where'-clause.");
            var values = "";
            foreach (var val in rec)
                values += $" {val.Key} = '{val.Value}',";
            values = values.Substring(0, values.Length - 1);
            ExecuteCommand($"update {tableName} set {values} where {whereClause};");
        }


    }
}
