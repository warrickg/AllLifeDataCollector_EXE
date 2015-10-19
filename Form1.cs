using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using Npgsql;
using System.IO;
using System.Xml;
using System.Collections;
using System.Data.SqlClient;
using AllLifeDataCollector_EXE;
using System.Net.Mail;
using System.Net;

namespace AllLifeDataCollector_EXE
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
        }

        public ArrayList strListOfStrings = new ArrayList();
        public ArrayList strListOfStringsUsers = new ArrayList();
        public ArrayList strListOfStringsPasswords = new ArrayList();
        public ArrayList strListOfStringsDatabases = new ArrayList();
        public ArrayList strListOfSQLServers = new ArrayList();
        private NpgsqlConnection PGSQLConnection1;
        private NpgsqlDataAdapter NpAdapter;
        private NpgsqlParameter NpParam = null;
        string strPGServerItemName = string.Empty;
        string strPGServerUsername = string.Empty;
        string strPGServerPassword = string.Empty;
        string strPGServerDB = string.Empty;
        string strSQLServerItemName = string.Empty;
        string strSQLServerUsername = string.Empty;
        string strSQLServerPassword = string.Empty;
        string strSQLServerDBName = string.Empty;
        string strSQLServerMonitoringItemName = string.Empty;
        string strSQLServerMonitoringUsername = string.Empty;
        string strSQLServerMonitoringPassword = string.Empty;
        string strSQLServerMonitoringDBName = string.Empty;
        string strTimerInterval = string.Empty;
        string strWriteAllEventToTheLogFile = string.Empty;
        int intNumberOfDaysTokeepLogFiles = 10;
        string strDestinationDirectory = string.Empty;
        string strAutoSync = string.Empty;
        string strCreatedDate = string.Empty;
        private string strLogPath = string.Empty;
        public bool blnLogEvents = false;
        int intJobID = 0;
        string strJobName = string.Empty;
        decimal decJobInterval = 0;
        bool blnJobEnabled = false;
        bool blnDoBulkInsert = false;

        private void button1_Click(object sender, EventArgs e)
        {
            DataSet ds = new DataSet();
            DataTable dt = new DataTable();
            Int32 intItemsProcessed = 0;
            string Item0 = string.Empty;
            string Item1 = string.Empty;
            string Item2 = string.Empty;
            string Item3 = string.Empty;
            string Item4 = string.Empty;
            string Item5 = string.Empty;
            string Item6 = string.Empty;
            int intJobID = 0;
            string strJobName = string.Empty;
            decimal decJobInterval = 0;
            bool blnJobEnabled = false;
            SqlConnection SQLConnectionMonitoring = new SqlConnection();
            SqlConnection SQLConnectionMonitoringUpdate = new SqlConnection();
            RepositoriesCollection Repositories = new RepositoriesCollection();
            JobCollection Jobs = new JobCollection();
            Int32 intRunID = 0;
            string strTableName = string.Empty;
            string strUniqueColumnName = string.Empty;
            //Int32 intLastTransaction = 0;
            string strLastTransaction = "0";
            string strSpecialCondition = string.Empty;
            string strSpecialConditionColumnName = string.Empty;
            string strSpecialConditionID = string.Empty;
            string strThrottle = string.Empty;
            string strRepopulateAtSpecificTime = string.Empty;
            string strLastRunDate = string.Empty;
       
            string strInsertSQLPrefix = string.Empty;
            string strInsertSQL = string.Empty;
            string strDeleteSQL = string.Empty;
            Int32 intColumnCount = 0;
            //Int32 intCurrentUniqueID = 0;
            string strCurrentUniqueID = "0";
            string strCurrentSpecialConditionID = "0";
            string strCmdMonitoring = string.Empty;
            SqlCommand myCommandMonitoring;
            SqlCommand myCommandMonitoringUpdate;
            ArrayList ALColumnName = new ArrayList();
            ArrayList ALDataType = new ArrayList();
            ArrayList ALDataLength = new ArrayList();
            string strTemp = string.Empty;
            DateTime dtCurrentTime = new DateTime();
            DateTime dtTargetTime = new DateTime();
            DateTime dtLastRunDate = new DateTime();
            DateTime timeLimit = new DateTime();
            string strTargetTime = string.Empty;
            string strTargetTimeHour = string.Empty;
            string strTargetTimeMinute = string.Empty;
            Boolean blnContinue = true;

            //SendNotificationEmail("Test");            

            try
            {
                //blnLogEvents = true;
                WriteTolog("Start of process", 1);
                WriteTolog("Timer elapsed event start", 0);
                WriteTolog("File location: " + System.AppDomain.CurrentDomain.BaseDirectory.ToString() + "SQLServerList.xml", 0);
                WriteTolog("Reading XML...", 0);
                XML_ReadFile(Application.StartupPath.ToString() + "\\SQLServerList.xml");
                WriteTolog("After XML_ReadFile", 0);
                WriteTolog("strPGServerItemName: " + strPGServerItemName, 0);
                WriteTolog("strSQLServerItemName: " + strSQLServerItemName, 0);
                WriteTolog("strTimerInterval: " + strTimerInterval, 0);

                string strSqlMonitoring = ("user id=" + strSQLServerMonitoringUsername + ";" +
                                "password=" + strSQLServerMonitoringPassword + ";server=" + strSQLServerMonitoringItemName + ";" +
                                //"Trusted_Connection=yes;" +
                                "database=" + strSQLServerMonitoringDBName + ";" +
                                "connection timeout=30");
                WriteTolog("strSql: " + strSqlMonitoring, 0);
                SQLConnectionMonitoring = new SqlConnection(strSqlMonitoring);
                WriteTolog("Opening SQLConnectionMonitoring Connection", 0);
                SQLConnectionMonitoring.Open();
                WriteTolog("Connection to SQLConnectionMonitoring was successfull", 0);
                SQLConnectionMonitoringUpdate = new SqlConnection(strSqlMonitoring);
                WriteTolog("Opening SQLConnectionMonitoringUpdate Connection", 0);
                SQLConnectionMonitoringUpdate.Open();
                WriteTolog("Connection to SQLConnectionMonitoringUpdate was successfull", 0);

                //throw new ApplicationException("This is a test test message");

                #region "Get Reporitories"
                //Get Repository info
                strCmdMonitoring = "SELECT RepositoryID,RepositoryType,RepositoryName,RepositoryServerName,RepositoryUsername,RepositoryPassword,RepositoryDBName FROM tbl_ServiceRepositories";
                WriteTolog("SQL:" + strCmdMonitoring, 0);
                myCommandMonitoring = new SqlCommand(strCmdMonitoring, SQLConnectionMonitoring);
                SqlDataReader readerMonitoring = myCommandMonitoring.ExecuteReader();
                if (readerMonitoring.HasRows)
                {
                    while (readerMonitoring.Read())
                    {
                        //Console.WriteLine("{0}\t{1}", reader.GetInt32(0), reader.GetString(1));
                        WriteTolog(readerMonitoring.GetInt32(0).ToString() + ", " + readerMonitoring.GetString(1) + ", " + readerMonitoring.GetString(2) + ", " + readerMonitoring.GetString(3) + ", " + readerMonitoring.GetString(4) + ", " + readerMonitoring.GetString(5) + ", " + readerMonitoring.GetString(6) + ", ", 0);
                        Item0 = readerMonitoring.GetInt32(0).ToString();
                        Item1 = readerMonitoring.GetString(1);
                        Item2 = readerMonitoring.GetString(2);
                        Item3 = readerMonitoring.GetString(3);
                        Item4 = readerMonitoring.GetString(4);
                        Item5 = readerMonitoring.GetString(5);
                        Item6 = readerMonitoring.GetString(6);

                        Repository CurrentRepository = new Repository();
                        CurrentRepository.RepositoryID = Convert.ToInt32(Item0);
                        CurrentRepository.RepositoryType = Item1;
                        CurrentRepository.RepositoryName = Item2;
                        CurrentRepository.RepositoryServerName = Item3;
                        CurrentRepository.RepositoryUsername = Item4;
                        CurrentRepository.RepositoryPassword = Item5;
                        CurrentRepository.RepositoryDBName = Item6;

                        Repositories.Add(CurrentRepository);

                        intItemsProcessed += 1;
                    }
                }
                else
                {
                    //Console.WriteLine("No rows found.");
                    WriteTolog("No rows found.", 0);
                }
                readerMonitoring.Close();
                readerMonitoring.Dispose();

                WriteTolog("Number of repositories: " + Repositories.Count.ToString(), 0);
                intItemsProcessed = 0;

                foreach (Repository RepositoryItem in Repositories)
                {
                    switch (RepositoryItem.RepositoryName)
                    {
                        case "Magnum":
                            strPGServerItemName = RepositoryItem.RepositoryServerName;
                            strPGServerUsername = RepositoryItem.RepositoryUsername;
                            strPGServerPassword = RepositoryItem.RepositoryPassword;
                            strPGServerDB = RepositoryItem.RepositoryDBName;
                            break;
                        case "ZReporting":
                            strSQLServerItemName = RepositoryItem.RepositoryServerName;
                            strSQLServerUsername = RepositoryItem.RepositoryUsername;
                            strSQLServerPassword = RepositoryItem.RepositoryPassword;
                            strSQLServerDBName = RepositoryItem.RepositoryDBName;
                            break;
                        case "DataCollector":
                            strSQLServerItemName = RepositoryItem.RepositoryServerName;
                            strSQLServerUsername = RepositoryItem.RepositoryUsername;
                            strSQLServerPassword = RepositoryItem.RepositoryPassword;
                            strSQLServerDBName = RepositoryItem.RepositoryDBName;
                            break;
                    }
                }

                intItemsProcessed = 0;
                #endregion

                #region "Get Jobs"
                //Get the Jobs that need to be processed
                strCmdMonitoring = "SELECT JobID, JobName, JobInterval, JobEnabled FROM tbl_ServiceJobs ORDER BY JobOrder";
                myCommandMonitoring = new SqlCommand(strCmdMonitoring, SQLConnectionMonitoring);
                SqlDataReader readerJobs = myCommandMonitoring.ExecuteReader();
                if (readerJobs.HasRows)
                {
                    while (readerJobs.Read())
                    {
                        WriteTolog(readerJobs.GetInt32(0).ToString() + ", " + readerJobs.GetString(1) + ", " + readerJobs.GetDecimal(2).ToString() + ", " + readerJobs.GetBoolean(3).ToString(), 0);
                        intJobID = readerJobs.GetInt32(0);
                        strJobName = readerJobs.GetString(1);
                        decJobInterval = readerJobs.GetDecimal(2);
                        blnJobEnabled = readerJobs.GetBoolean(3);

                        Job CurrentJob = new Job();
                        CurrentJob.JobID = intJobID;
                        CurrentJob.JobName = strJobName;
                        CurrentJob.JobInterval = decJobInterval;
                        //This will need to be changed if we add more jobs because currently this is setting the timers interval
                        //And because we only have one job we can use the timer's interval to do the wating
                        strTimerInterval = CurrentJob.JobInterval.ToString();
                        CurrentJob.JobEnabled = blnJobEnabled;

                        Jobs.Add(CurrentJob);
                    }
                }

                WriteTolog("Number of Jobs: " + Jobs.Count.ToString(), 0);

                readerJobs.Close();
                readerJobs.Dispose();

                intItemsProcessed = 0;    
                #endregion            

                #region "Connect Database Connections"
                string connstring = String.Format("Server=" + strPGServerItemName + ";Port=5432;" + "User Id=" + strPGServerUsername + ";Password=" + strPGServerPassword + ";Pooling=false;Database=" + strPGServerDB + ";CommandTimeout=600;");
                WriteTolog("connstring: " + connstring, 0);
                // Making connection with Npgsql provider
                NpgsqlConnection PGSQLConnection1 = new NpgsqlConnection(connstring);
                PGSQLConnection1.Open();
                WriteTolog("Connection to Postgres was successfull", 0);

                string strSql = ("user id=" + strSQLServerUsername + ";" +
                                "password=" + strSQLServerPassword + ";server=" + strSQLServerItemName + ";" +
                                "Trusted_Connection=yes;" +
                                "database=" + strSQLServerDBName + ";" +
                                "connection timeout=30");
                WriteTolog("strSql: " + strSql, 0);
                SqlConnection SQLConnection1 = new SqlConnection(strSql);
                WriteTolog("Opening SQL Connection", 0);
                SQLConnection1.Open();
                WriteTolog("Connection to SQL was successfull", 0);
                #endregion

                #region "Process each Job"
                //Check job intervals and process jobs if they should be processed
                foreach (Job JobItem in Jobs)
                {
                    intJobID = JobItem.JobID;
                    strJobName = JobItem.JobName;
                    decJobInterval = JobItem.JobInterval;
                    blnJobEnabled = JobItem.JobEnabled;

                    #region "Update Monitoring table"

                    strCmdMonitoring = "SELECT Count(RunID) FROM tbl_ServiceMonitor WHERE JobID = " + intJobID.ToString();
                    WriteTolog(strCmdMonitoring, 0);
                    using (myCommandMonitoring = new SqlCommand(strCmdMonitoring, SQLConnectionMonitoring))
                    {
                        intRunID = Convert.ToInt32(myCommandMonitoring.ExecuteScalar().ToString());
                    }

                    if (intRunID == 0)
                    {
                        strCmdMonitoring = "INSERT INTO tbl_ServiceMonitor" +
                           " ([JobID],[StartDateTime],[Status])" +
                           " VALUES (" + intJobID.ToString() + ", '" + System.DateTime.Now.ToString("u").Replace("Z", "") + "', 2);" +
                           " Select @@IDENTITY as newId;";
                        WriteTolog(strCmdMonitoring, 0);
                        using (myCommandMonitoring = new SqlCommand(strCmdMonitoring, SQLConnectionMonitoring))
                        {
                            intRunID = Convert.ToInt32(myCommandMonitoring.ExecuteScalar().ToString());
                        }
                    }
                    else
                    {
                        strCmdMonitoring = "SELECT RunID FROM tbl_ServiceMonitor WHERE JobID = " + intJobID.ToString();
                        WriteTolog(strCmdMonitoring, 0);
                        using (myCommandMonitoring = new SqlCommand(strCmdMonitoring, SQLConnectionMonitoring))
                        {
                            intRunID = Convert.ToInt32(myCommandMonitoring.ExecuteScalar().ToString());
                        }

                        strCmdMonitoring = "UPDATE tbl_ServiceMonitor" +
                           " SET [StartDateTime] = '" + System.DateTime.Now.ToString("u").Replace("Z", "") + "', [EndDateTime] = null" +
                           ", [Status] = 2 WHERE RunID = " + intRunID.ToString();
                        WriteTolog(strCmdMonitoring, 0);
                        myCommandMonitoring = new SqlCommand(strCmdMonitoring, SQLConnectionMonitoring);
                        myCommandMonitoring.ExecuteNonQuery();
                    }

                    #endregion

                    if (blnJobEnabled == true)
                    {
                        //strCmdMonitoring = "SELECT RunID, JobID, StartDateTime, EndDateTime, Status FROM tbl_ServiceMonitor WHERE JobID = " + intJobID.ToString() + " and EndDateTime is null";

                        //myCommandMonitoring = new SqlCommand(strCmdMonitoring, SQLConnectionMonitoring);
                        //SqlDataReader readerStatus = myCommandMonitoring.ExecuteReader();
                        //if (readerMonitoring.HasRows)
                        //{
                        //    while (readerMonitoring.Read())
                        //    {
                        //    }
                        //}

                        //Get all the tables for this job and move the data for each table
                        string strCmdGetTables = "SELECT TableName, UniqueColumnName, LastUID, isnull(SpecialCondition,0), isnull(SpecialConditionColumnName,0), IsNull(SpecialConditionID,0), Throttle, IsNull(RepopulateAtSpecificTime,0), IsNull(LastRunDate,0) FROM tbl_ServiceTables WHERE Enabled = 1 and JobID = " + intJobID.ToString();
                        SqlCommand myCommandMGetTables = new SqlCommand(strCmdGetTables, SQLConnectionMonitoring);
                        SqlDataReader readerGetTables = myCommandMGetTables.ExecuteReader();
                        if (readerGetTables.HasRows)
                        {
                            //loop through the tables
                            while (readerGetTables.Read())
                            {
                                //WriteTolog(readerGetTables.GetString(0), 0);
                                strTableName = readerGetTables.GetString(0);
                                strUniqueColumnName = readerGetTables.GetString(1);
                                //intLastTransaction = readerGetTables.GetInt32(2);
                                strLastTransaction = readerGetTables.GetString(2);
                                strSpecialCondition = readerGetTables.GetString(3);
                                strSpecialConditionColumnName = readerGetTables.GetString(4);
                                strSpecialConditionID = readerGetTables.GetString(5);
                                strThrottle = readerGetTables.GetInt32(6).ToString();

                                if (!readerGetTables.IsDBNull(7))
                                {                                    
                                    strRepopulateAtSpecificTime = readerGetTables.GetString(7).ToString();
                                }
                                else
                                {
                                    strRepopulateAtSpecificTime = "0";
                                }

                                if (!readerGetTables.IsDBNull(8))
                                {
                                    strLastRunDate = readerGetTables.GetString(8).ToString();

                                    if (strLastRunDate == "0")
                                    {
                                        dtCurrentTime = System.DateTime.Now;
                                        dtLastRunDate = new DateTime(dtCurrentTime.Year, dtCurrentTime.Month, dtCurrentTime.Day, Convert.ToInt16(0), Convert.ToInt16(0), 0);
                                        dtLastRunDate = dtLastRunDate.AddDays(-2);
                                        strLastRunDate = dtLastRunDate.ToString();
                                    }
                                }
                                else
                                {
                                    dtCurrentTime = System.DateTime.Now;
                                    dtLastRunDate = new DateTime(dtCurrentTime.Year, dtCurrentTime.Month, dtCurrentTime.Day - 2, Convert.ToInt16(0), Convert.ToInt16(0), 0);
                                    strLastRunDate = dtLastRunDate.ToString();
                                }
                                dtLastRunDate = Convert.ToDateTime(strLastRunDate);
                                WriteTolog("dtLastRunDate: " + dtLastRunDate, 0);

                                WriteTolog("__________________________________________", 1);
                                WriteTolog("Processing table: " + strTableName, 1);
                                //WriteTolog("-----------------------------------------", 1);

                                //select column_name, Data_type, Character_maximum_length from information_schema.columns where table_name='member';
                                #region "Get the tables data and insert it into the SQL table"
                                //ALColumnNames.Clear();
                                //ALDataTypes.Clear();
                                //ALLengths.Clear();
                                //string sql = "SELECT * FROM " + strTableName;
                                string sql = "select column_name, Data_type, Character_maximum_length from information_schema.columns where table_name='" + strTableName + "' ORDER BY ordinal_Position;";
                                WriteTolog("Postgres: " + sql, 0);
                                NpgsqlDataAdapter adapterGetColumns = new NpgsqlDataAdapter(sql, PGSQLConnection1);

                                DataSet DSColumns = new DataSet();
                                adapterGetColumns.Fill(DSColumns, "Columns");

                                strInsertSQLPrefix = "INSERT INTO " + strTableName + " (";

                                intColumnCount = 1;
                                WriteTolog("Clear Array Lists", 0);
                                ALColumnName.Clear();
                                ALDataType.Clear();
                                ALDataLength.Clear();

                                WriteTolog("Starting loop through columns", 0);
                                foreach (DataRow row in DSColumns.Tables[0].Rows)
                                {
                                    //ALColumnNames.Add(row[0].ToString());
                                    //ALDataTypes.Add(row[1].ToString());
                                    //ALLengths.Add(row[2].ToString());

                                    strInsertSQLPrefix += row[0].ToString();
                                    ALColumnName.Add(row[0].ToString());
                                    ALDataType.Add(row[1].ToString());
                                    ALDataLength.Add(row[2].ToString());

                                    if (DSColumns.Tables[0].Rows.Count > 1)
                                    {
                                        if (intColumnCount != DSColumns.Tables[0].Rows.Count)
                                            strInsertSQLPrefix += ",";
                                    }

                                    intColumnCount += 1;
                                }
                                WriteTolog("Completed loop through columns", 0);

                                strInsertSQLPrefix += ") Values ('";


                                //throw new ApplicationException("This is a test test message");

                                blnContinue = true;
                                blnDoBulkInsert = false;

                                switch (strSpecialCondition)
                                {
                                    case "0":
                                        if (strRepopulateAtSpecificTime != "0")
                                        {
                                            //int intPos = strSpecialConditionColumnName.IndexOf("=");
                                            dtCurrentTime = System.DateTime.Now;
                                            //strTargetTime = strSpecialConditionColumnName.Substring(intPos + 1);
                                            strTargetTime = strRepopulateAtSpecificTime;
                                            strTargetTimeHour = strTargetTime.Substring(0, 2);
                                            strTargetTimeMinute = strTargetTime.Substring(3, 2);
                                            timeLimit = new DateTime(dtCurrentTime.Year, dtCurrentTime.Month, dtCurrentTime.Day, Convert.ToInt16(strTargetTimeHour), Convert.ToInt16(strTargetTimeMinute), 0);
                                            WriteTolog("dtCurrentTime: " + dtCurrentTime.ToString(), 1);
                                            WriteTolog("timeLimit: " + timeLimit.ToString(), 1);
                                            if (dtCurrentTime > timeLimit)
                                            {
                                                WriteTolog("dtCurrentTime > timeLimit", 0);
                                                TimeSpan sinceLastRun = DateTime.Now - dtLastRunDate;
                                                WriteTolog("sinceLastRun: " + sinceLastRun.ToString(), 0);
                                                if (sinceLastRun.TotalHours > 23)
                                                {
                                                    WriteTolog("dtLastRunDate:" + dtLastRunDate.ToString(), 1);
                                                    WriteTolog("sinceLastRun.TotalHours > 23", 0);
                                                    sql = "TRUNCATE TABLE " + strTableName;
                                                    WriteTolog("sql: " + sql, 0);
                                                    SqlCommand myCommand = new SqlCommand(sql, SQLConnection1);
                                                    blnContinue = true;
                                                    myCommand.ExecuteNonQuery();
                                                    blnDoBulkInsert = true;
                                                    WriteTolog("Table has been Truncated: " + strTableName, 0);
                                                }
                                                else
                                                {
                                                    WriteTolog("dtLastRunDate:" + dtLastRunDate.ToString(), 1);
                                                    WriteTolog("sinceLastRun.TotalHours !> 23", 1);                                                    
                                                    blnDoBulkInsert = false;
                                                }
                                            }
                                            else
                                            {
                                                WriteTolog("dtCurrentTime !> timeLimit", 1);
                                                blnDoBulkInsert = false;
                                            }
                                        }

                                        sql = "SELECT * FROM " + strTableName;

                                        if (blnDoBulkInsert == false)
                                        {
                                            sql = sql + " WHERE " + strUniqueColumnName + " > '" + strLastTransaction.ToString() + "' ORDER BY " + strUniqueColumnName;

                                            //We ignore the limit so that all the rows are replaced and then when
                                            //there job runs normally for this table it will start copying from the last uid again
                                            if (strThrottle != "0")
                                            {
                                                sql += " LIMIT " + strThrottle;
                                            }
                                        }
                                        break;
                                    case "ReplaceAll":
                                        //Run the query to truncate the table
                                        if (strSpecialCondition == "ReplaceAll")
                                        {
                                            #region "ReplaceAll"
                                            //if (strSpecialConditionColumnName.Contains("RunTime="))
                                            if (strRepopulateAtSpecificTime != "0")
                                            {                                                
                                                //int intPos = strSpecialConditionColumnName.IndexOf("=");
                                                dtCurrentTime = System.DateTime.Now;
                                                //strTargetTime = strSpecialConditionColumnName.Substring(intPos + 1);
                                                strTargetTime = strRepopulateAtSpecificTime;
                                                strTargetTimeHour = strTargetTime.Substring(0, 2);
                                                strTargetTimeMinute = strTargetTime.Substring(3, 2);
                                                timeLimit = new DateTime(dtCurrentTime.Year, dtCurrentTime.Month, dtCurrentTime.Day, Convert.ToInt16(strTargetTimeHour), Convert.ToInt16(strTargetTimeMinute), 0);
                                                WriteTolog("dtCurrentTime: " + dtCurrentTime.ToString(), 1);
                                                WriteTolog("timeLimit: " + timeLimit.ToString(), 1);
                                                if (dtCurrentTime > timeLimit)
                                                {
                                                    WriteTolog("dtCurrentTime > timeLimit", 0);
                                                    TimeSpan sinceLastRun = DateTime.Now - dtLastRunDate;
                                                    WriteTolog("sinceLastRun: " + sinceLastRun.ToString(), 0);
                                                    if (sinceLastRun.TotalHours > 23)
                                                    {
                                                        WriteTolog("dtLastRunDate:" + dtLastRunDate.ToString(), 1);
                                                        WriteTolog("sinceLastRun.TotalHours > 23", 0);
                                                        sql = "TRUNCATE TABLE " + strTableName;
                                                        WriteTolog("sql: " + sql, 0);
                                                        SqlCommand myCommand = new SqlCommand(sql, SQLConnection1);
                                                        blnContinue = true;
                                                        myCommand.ExecuteNonQuery();
                                                        blnDoBulkInsert = true;
                                                        WriteTolog("Table has been Truncated: " + strTableName, 0);
                                                    }
                                                    else
                                                    {
                                                        WriteTolog("dtLastRunDate:" + dtLastRunDate.ToString(), 1);
                                                        WriteTolog("sinceLastRun.TotalHours !> 23", 1);
                                                        blnContinue = false;
                                                        blnDoBulkInsert = false;
                                                    }
                                                }
                                                else
                                                {
                                                    WriteTolog("dtCurrentTime !> timeLimit", 1);
                                                    blnContinue = false;
                                                    blnDoBulkInsert = false;
                                                }                                            
                                            }
                                            else
                                            {
                                                sql = "TRUNCATE TABLE " + strTableName;
                                                WriteTolog("sql: " + sql, 0);
                                                SqlCommand myCommand = new SqlCommand(sql, SQLConnection1);
                                                myCommand.ExecuteNonQuery();
                                            }
                                            #endregion
                                        }

                                        sql = "SELECT * FROM " + strTableName;
                                        break;
                                    case "DateDriven":                                        
                                        string strcolumnNames = string.Empty;
                                        string strcolumnName = string.Empty;

                                        #region "DateDriven"
                                        for (int i = 0; i < ALColumnName.Count; i++)
                                        {
                                            //if (i == 0)
                                            //    strcolumnNames = "'";

                                            if (i != ALColumnName.Count)
                                            {
                                                if (i > 0)
                                                    strcolumnNames += ",";
                                            }

                                            if (ALColumnName[i].ToString() == strSpecialConditionColumnName)
                                            {
                                                //strcolumnName += "to_char(" + ALColumnName[i].ToString() + ", 'YYYY-MM-DD HH24:MI:SS')";
                                                strcolumnNames += "to_char(" + ALColumnName[i].ToString() + ", 'YYYY-MM-DD HH24:MI:SS') as \"" + strSpecialConditionColumnName + "\"";
                                            }
                                            else
                                            {
                                                strcolumnNames += ALColumnName[i].ToString();
                                            }
                                        }
                                        //sql = "SELECT " + strcolumnNames + " FROM " + strTableName + " WHERE " + strUniqueColumnName + " > '" + strLastTransaction + "' and " + strSpecialConditionColumnName + " is not null and " + strSpecialConditionColumnName + " > '" + strSpecialConditionID.ToString() + "' ORDER BY " + strSpecialConditionColumnName;
                                        sql = "SELECT " + strcolumnNames + " FROM " + strTableName + " WHERE " + strSpecialConditionColumnName + " is not null and to_char(" + strSpecialConditionColumnName + ", 'YYYY-MM-DD HH24:MI:SS') > '" + strSpecialConditionID.ToString() + "' ORDER BY " + strSpecialConditionColumnName;
                                        #endregion
                                        break;
                                    case "ReplaceRowsBasedOnDate":
                                        string strcolumnNames2 = string.Empty;
                                        string strcolumnName2 = string.Empty;
                                        #region "ReplaceRowsBasedOnDate"
                                        if (strRepopulateAtSpecificTime != "0")
                                        {
                                            #region "if (strRepopulateAtSpecificTime != 0)"
                                            //int intPos = strSpecialConditionColumnName.IndexOf("=");
                                            dtCurrentTime = System.DateTime.Now;
                                            //strTargetTime = strSpecialConditionColumnName.Substring(intPos + 1);
                                            strTargetTime = strRepopulateAtSpecificTime;
                                            strTargetTimeHour = strTargetTime.Substring(0, 2);
                                            strTargetTimeMinute = strTargetTime.Substring(3, 2);
                                            timeLimit = new DateTime(dtCurrentTime.Year, dtCurrentTime.Month, dtCurrentTime.Day, Convert.ToInt16(strTargetTimeHour), Convert.ToInt16(strTargetTimeMinute), 0);
                                            WriteTolog("dtCurrentTime: " + dtCurrentTime.ToString(), 1);
                                            WriteTolog("timeLimit: " + timeLimit.ToString(), 1);
                                            if (dtCurrentTime > timeLimit)
                                            {
                                                WriteTolog("dtCurrentTime > timeLimit", 0);
                                                TimeSpan sinceLastRun = DateTime.Now - dtLastRunDate;
                                                WriteTolog("sinceLastRun: " + sinceLastRun.ToString(), 0);
                                                if (sinceLastRun.TotalHours > 23)
                                                {
                                                    WriteTolog("dtLastRunDate:" + dtLastRunDate.ToString(), 1);
                                                    WriteTolog("sinceLastRun.TotalHours > 23", 0);
                                                    sql = "TRUNCATE TABLE " + strTableName;
                                                    WriteTolog("sql: " + sql, 0);
                                                    SqlCommand myCommand = new SqlCommand(sql, SQLConnection1);
                                                    blnContinue = true;
                                                    myCommand.ExecuteNonQuery();
                                                    blnDoBulkInsert = true;
                                                    WriteTolog("Table has been Truncated: " + strTableName, 0);
                                                    sql = "SELECT * FROM " + strTableName;

                                                    if (strThrottle != "0")
                                                    {
                                                        sql += " LIMIT " + strThrottle;
                                                    }
                                                }
                                                else
                                                {
                                                    WriteTolog("dtLastRunDate:" + dtLastRunDate.ToString(), 1);
                                                    WriteTolog("sinceLastRun.TotalHours !> 23", 1);
                                                    for (int i = 0; i < ALColumnName.Count; i++)
                                                    {
                                                        if (i != ALColumnName.Count)
                                                        {
                                                            if (i > 0)
                                                                strcolumnNames2 += ",";
                                                        }

                                                        if (ALColumnName[i].ToString() == strSpecialConditionColumnName)
                                                        {
                                                            strcolumnNames2 += "to_char(" + ALColumnName[i].ToString() + ", 'YYYY-MM-DD HH24:MI:SS') as \"" + strSpecialConditionColumnName + "\"";
                                                        }
                                                        else
                                                        {
                                                            strcolumnNames2 += ALColumnName[i].ToString();
                                                        }
                                                    }
                                                    if (strSpecialConditionID == "0")
                                                        strSpecialConditionID = "1900-01-01";

                                                    sql = "SELECT " + strcolumnNames2 + " FROM " + strTableName + " WHERE to_char(" + strSpecialConditionColumnName + ", 'YYYY-MM-DD HH24:MI:SS') > '" + strSpecialConditionID.ToString() + "' ORDER BY " + strSpecialConditionColumnName;
                                                    blnDoBulkInsert = false;
                                                }
                                            }
                                            else
                                            {
                                                WriteTolog("dtCurrentTime !> timeLimit", 1);
                                                for (int i = 0; i < ALColumnName.Count; i++)
                                                {
                                                    if (i != ALColumnName.Count)
                                                    {
                                                        if (i > 0)
                                                            strcolumnNames2 += ",";
                                                    }

                                                    if (ALColumnName[i].ToString() == strSpecialConditionColumnName)
                                                    {
                                                        strcolumnNames2 += "to_char(" + ALColumnName[i].ToString() + ", 'YYYY-MM-DD HH24:MI:SS') as \"" + strSpecialConditionColumnName + "\"";
                                                    }
                                                    else
                                                    {
                                                        strcolumnNames2 += ALColumnName[i].ToString();
                                                    }
                                                }
                                                if (strSpecialConditionID == "0")
                                                    strSpecialConditionID = "1900-01-01";

                                                sql = "SELECT " + strcolumnNames2 + " FROM " + strTableName + " WHERE to_char(" + strSpecialConditionColumnName + ", 'YYYY-MM-DD HH24:MI:SS') > '" + strSpecialConditionID.ToString() + "' ORDER BY " + strSpecialConditionColumnName;
                                                blnDoBulkInsert = false;
                                            }
                                            #endregion
                                        }
                                        else
                                        {
                                            #region "if (strRepopulateAtSpecificTime == 0)"
                                            for (int i = 0; i < ALColumnName.Count; i++)
                                            {
                                                if (i != ALColumnName.Count)
                                                {
                                                    if (i > 0)
                                                        strcolumnNames2 += ",";
                                                }

                                                if (ALColumnName[i].ToString() == strSpecialConditionColumnName)
                                                {
                                                    strcolumnNames2 += "to_char(" + ALColumnName[i].ToString() + ", 'YYYY-MM-DD HH24:MI:SS') as \"" + strSpecialConditionColumnName + "\"";
                                                }
                                                else
                                                {
                                                    strcolumnNames2 += ALColumnName[i].ToString();
                                                }
                                            }
                                            if (strSpecialConditionID == "0")
                                                strSpecialConditionID = "1900-01-01";

                                            sql = "SELECT " + strcolumnNames2 + " FROM " + strTableName + " WHERE to_char(" + strSpecialConditionColumnName + ", 'YYYY-MM-DD HH24:MI:SS') > '" + strSpecialConditionID.ToString() + "' ORDER BY " + strSpecialConditionColumnName;
                                            blnDoBulkInsert = false;

                                            if (strThrottle != "0")
                                            {
                                                sql += " LIMIT " + strThrottle;
                                            }
                                            #endregion
                                        }
                                        #endregion
                                        break;
                                    default:
                                        break;
                                }

                                if (blnContinue == true)
                                {
 
                                    WriteTolog("sql: " + sql, 0);
                                    //NpgsqlDataAdapter adapterGetData = new NpgsqlDataAdapter(sql, PGSQLConnection1);

                                    DataSet DSData = new DataSet();
                                    //adapterGetData.Fill(DSData, "Data");

                                    using (NpgsqlCommand NpgCommand = new NpgsqlCommand(sql, PGSQLConnection1))
                                    {
                                        using (NpgsqlDataReader myReader = NpgCommand.ExecuteReader())
                                        {
                                            DataTable myTable = new DataTable();
                                            myTable.Load(myReader);
                                            DSData.Tables.Add(myTable);
                                        }
                                    }

                                    intItemsProcessed = 0;

                                    WriteTolog("Number of rows to process: " + DSData.Tables[0].Rows.Count.ToString(), 1);

                                    //if ((strSpecialCondition == "ReplaceAll") && (strSpecialConditionColumnName.Contains("RunTime=")))
                                    //if ((strSpecialCondition == "ReplaceAll") && (strRepopulateAtSpecificTime != ""))
                                    //if (strRepopulateAtSpecificTime != "0")
                                    if (blnDoBulkInsert == true)
                                    {
                                        //for (int i = 0; i < DSData.Tables[0].Columns.Count; i++)
                                        //{
                                        //    string col = DSData.Tables[0].Columns[i].DataType.ToString();
                                        //    string colleng = DSData.Tables[0].Columns[i].MaxLength.ToString();

                                        //    if (col == "System.Boolean")
                                        //    {
                                        //        DSData.Tables[0].Columns[i].DataType = typeof(string);
                                        //    }
                                        //}

                                        WriteTolog("Re-populating table: " + strTableName, 1);
                                        using (SqlConnection cn = new SqlConnection(strSql))
                                        {
                                            cn.Open();
                                            using (SqlBulkCopy copy = new SqlBulkCopy(cn))
                                            {
                                                copy.DestinationTableName = strTableName;

                                                foreach (string strColName in ALColumnName)
                                                {
                                                    copy.ColumnMappings.Add(strColName.Trim(), strColName.Trim());
                                                }

                                                copy.WriteToServer(DSData.Tables[0]);
                                            }

                                            SqlCommand myCommand = new SqlCommand("UPDATE tbl_ServiceTables SET LastRunDate = '" + System.DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "' WHERE TableName = '" + strTableName + "'", cn);
                                            myCommand.ExecuteNonQuery();
                                            cn.Close();
                                            cn.Dispose();
                                            WriteTolog("Replaced " + DSData.Tables[0].Rows.Count.ToString() + " rows",1);
                                        }
                                    }
                                    else
                                    {
                                        foreach (DataRow row in DSData.Tables[0].Rows)
                                        {                                            
                                            intColumnCount = 1;
                                            //WriteTolog("intColumnCount: " + intColumnCount.ToString(), 0);

                                            strInsertSQL = strInsertSQLPrefix;
                                            strTemp = string.Empty;

                                            //loop through all the columns in the current row
                                            for (int i = 0; i < row.ItemArray.Count(); i++)
                                            {
                                                string s = row[i].ToString();
                                                if (s == "31/03/2014 12:00:00 AM")
                                                {
                                                    string w = "";
                                                }
                                                //WriteTolog("i: " + i.ToString() + " | column: " + DSData.Tables[0].Columns[intColumnCount - 1].ColumnName.ToString(), 0);
                                                if (DSData.Tables[0].Columns[intColumnCount - 1].ColumnName == strUniqueColumnName)
                                                {
                                                    //intCurrentUniqueID = Convert.ToInt32(row[i].ToString());
                                                    strCurrentUniqueID = (row[i].ToString());

                                                    //WriteTolog("intCurrentUniqueID: " + intCurrentUniqueID.ToString(), 0);
                                                }

                                                if ((strSpecialCondition == "DateDriven") || (strSpecialCondition == "ReplaceRowsBasedOnDate"))
                                                {
                                                    if (DSData.Tables[0].Columns[intColumnCount - 1].ColumnName == strSpecialConditionColumnName)
                                                    {
                                                        //WriteTolog("intCurrentUniqueID: " + intCurrentUniqueID.ToString(), 0);
                                                        DateTime newDate = Convert.ToDateTime(row[i].ToString());
                                                        #region "Get Date in correct format"
                                                        //strCurrentSpecialConditionID = newDate.Year + "-" + newDate.Month + "-" + newDate.Day + " " + newDate.Hour + ":" + newDate.Minute + ":" + newDate.Second;
                                                        string strMonth = string.Empty;
                                                        string strDay = string.Empty;
                                                        string strHour = string.Empty;
                                                        string strMinute = string.Empty;
                                                        string strSecond = string.Empty;

                                                        if (newDate.Month < 10)
                                                            strMonth = "0" + newDate.Month.ToString();
                                                        else
                                                            strMonth = newDate.Month.ToString();

                                                        if (newDate.Day < 10)
                                                            strDay = "0" + newDate.Day.ToString();
                                                        else
                                                            strDay = newDate.Day.ToString();

                                                        if (newDate.Hour < 10)
                                                            strHour = "0" + newDate.Hour.ToString();
                                                        else
                                                            strHour = newDate.Hour.ToString();

                                                        if (newDate.Minute < 10)
                                                            strMinute = "0" + newDate.Minute.ToString();
                                                        else
                                                            strMinute = newDate.Minute.ToString();

                                                        if (newDate.Second < 10)
                                                            strSecond = "0" + newDate.Second.ToString();
                                                        else
                                                            strSecond = newDate.Second.ToString();
                                                        #endregion
                                                        //strInsertSQL += newDate.Year + "-" + strMonth + "-" + strDay + " " + strHour + ":" + strMinute + ":" + strSecond;
                                                        strCurrentSpecialConditionID = newDate.Year + "-" + strMonth + "-" + strDay + " " + strHour + ":" + strMinute + ":" + strSecond;
                                                    }
                                                }

                                                //If the item is a date then create the date else do string adjustments
                                                if (isDate(row[i].ToString()) == true)
                                                {
                                                    if (ALDataType[intColumnCount - 1].ToString().Trim() == "timestamp without time zone")
                                                    {
                                                        DateTime newDate = Convert.ToDateTime(row[i].ToString());
                                                        #region "Get Date in correct format"
                                                        string strMonth = string.Empty;
                                                        string strDay = string.Empty;
                                                        string strHour = string.Empty;
                                                        string strMinute = string.Empty;
                                                        string strSecond = string.Empty;

                                                        if (newDate.Month < 10)
                                                            strMonth = "0" + newDate.Month.ToString();
                                                        else
                                                            strMonth = newDate.Month.ToString();

                                                        if (newDate.Day < 10)
                                                            strDay = "0" + newDate.Day.ToString();
                                                        else
                                                            strDay = newDate.Day.ToString();

                                                        if (newDate.Hour < 10)
                                                            strHour = "0" + newDate.Hour.ToString();
                                                        else
                                                            strHour = newDate.Hour.ToString();

                                                        if (newDate.Minute < 10)
                                                            strMinute = "0" + newDate.Minute.ToString();
                                                        else
                                                            strMinute = newDate.Minute.ToString();

                                                        if (newDate.Second < 10)
                                                            strSecond = "0" + newDate.Second.ToString();
                                                        else
                                                            strSecond = newDate.Second.ToString();

                                                        #endregion
                                                        strInsertSQL += newDate.Year + "-" + strMonth + "-" + strDay + " " + strHour + ":" + strMinute + ":" + strSecond;
                                                    }
                                                    else if (ALDataType[intColumnCount - 1].ToString().Trim() == "date")
                                                    {
                                                        //strInsertSQL += row[i].ToString();

                                                        DateTime newDate = Convert.ToDateTime(row[i].ToString());
                                                        #region "Get Date in correct format"
                                                        string strMonth = string.Empty;
                                                        string strDay = string.Empty;
                                                        string strHour = string.Empty;
                                                        string strMinute = string.Empty;
                                                        string strSecond = string.Empty;

                                                        if (newDate.Month < 10)
                                                            strMonth = "0" + newDate.Month.ToString();
                                                        else
                                                            strMonth = newDate.Month.ToString();

                                                        if (newDate.Day < 10)
                                                            strDay = "0" + newDate.Day.ToString();
                                                        else
                                                            strDay = newDate.Day.ToString();

                                                        #endregion
                                                        strInsertSQL += newDate.Year + "-" + strMonth + "-" + strDay;
                                                    }
                                                    else
                                                    {
                                                        strInsertSQL += row[i].ToString();
                                                    }
                                                }
                                                else
                                                {
                                                    switch (ALDataType[intColumnCount - 1].ToString().Trim())
                                                    {
                                                        case "character":
                                                            strInsertSQL += row[i].ToString().Substring(0, Convert.ToInt16(ALDataLength[intColumnCount - 1]));
                                                            break;
                                                        case "character varying":
                                                            strTemp = row[i].ToString();
                                                            strTemp = strTemp.Replace("\"", "");

                                                            if (row[i].ToString().Contains("'") == true)
                                                            {
                                                                strInsertSQL += strTemp.Replace("'", "");
                                                            }
                                                            else
                                                            {
                                                                strInsertSQL += strTemp;
                                                            }
                                                            break;
                                                        //case "double precision":

                                                        //    string p = "";
                                                        //    break;
                                                        case "boolean":
                                                            if (row[i].ToString().Length > 0)
                                                            {
                                                                //2012-05-29 - WG - The below line was replaced by the 8 lines (two if statements)
                                                                //so that the SQL table columns data type can be a bit instead of a char(1)
                                                                //strInsertSQL += row[i].ToString().Substring(0, 1);
                                                                if (row[i].ToString().Substring(0, 1) == "T")
                                                                {
                                                                    strInsertSQL += "1";
                                                                }
                                                                if (row[i].ToString().Substring(0, 1) == "F")
                                                                {
                                                                    strInsertSQL += "0";
                                                                }
                                                            }
                                                            else
                                                                strInsertSQL += null;
                                                            break;
                                                        //case "date":
                                                        //    //2014-02-26 - WG - Added the line below to handel date data types
                                                        //    strTemp = row[i].ToString();
                                                        //    strTemp = strTemp.Replace("\"", "");

                                                        //    if (row[i].ToString().Contains("'") == true)
                                                        //    {
                                                        //        strInsertSQL += strTemp.Replace("'", "");
                                                        //    }
                                                        //    else
                                                        //    {
                                                        //        strInsertSQL += strTemp;
                                                        //    }
                                                        //    break;
                                                        default:
                                                            strInsertSQL += row[i].ToString();
                                                            break;
                                                    }
                                                    //if (ALDataType[intColumnCount - 1].ToString().Trim() == "character")
                                                    //{
                                                    //    strInsertSQL += row[i].ToString().Substring(0, Convert.ToInt16(ALDataLength[intColumnCount - 1]));
                                                    //}
                                                    //else
                                                    //{
                                                    //    strInsertSQL += row[i].ToString();
                                                    //}

                                                }

                                                if (intColumnCount == DSData.Tables[0].Columns.Count)
                                                {
                                                    strInsertSQL += "'";
                                                }
                                                else
                                                {
                                                    if (DSData.Tables[0].Columns.Count > 1)
                                                    {
                                                        strInsertSQL += "','";
                                                    }
                                                    else
                                                    {
                                                        strInsertSQL += "'";
                                                    }
                                                }

                                                intColumnCount += 1;
                                            }

                                            strInsertSQL += ")";

                                            //If the table is using the special condition "ReplaceRowsBasedOnDate"
                                            //Then we need to delete the row from the table before we insert the updated row
                                            if (strSpecialCondition == "ReplaceRowsBasedOnDate")
                                            {
                                                strDeleteSQL = "Delete FROM " + strTableName + " WHERE " + strUniqueColumnName + " = '" + strCurrentUniqueID + "'";
                                                WriteTolog("strDeleteSQL: " + strDeleteSQL, 0);
                                                SqlCommand myDelCommand = new SqlCommand(strDeleteSQL, SQLConnection1);
                                                myDelCommand.ExecuteNonQuery();
                                                myDelCommand.Dispose();
                                            }

                                            //Run the query to insert into the cube table
                                            WriteTolog("strInsertSQL: " + strInsertSQL, 0);
                                            SqlCommand myCommand = new SqlCommand(strInsertSQL, SQLConnection1);
                                            myCommand.ExecuteNonQuery();

                                            switch (strSpecialCondition)
                                            {
                                                case "0":
                                                    strCmdMonitoring = "UPDATE tbl_ServiceTables" +
                                                        //" SET LastUID = " + intCurrentUniqueID.ToString() +
                                                  " SET LastUID = '" + strCurrentUniqueID + "'" +
                                                  " WHERE TableName = '" + strTableName + "'";
                                                    WriteTolog(strCmdMonitoring, 0);
                                                    myCommandMonitoringUpdate = new SqlCommand(strCmdMonitoring, SQLConnectionMonitoringUpdate);
                                                    myCommandMonitoringUpdate.ExecuteNonQuery();
                                                    break;
                                                case "DateDriven":
                                                    strCmdMonitoring = "UPDATE tbl_ServiceTables" +
                                                  " SET SpecialConditionID = '" + strCurrentSpecialConditionID + "'" +
                                                  " WHERE TableName = '" + strTableName + "'";
                                                    WriteTolog(strCmdMonitoring, 0);
                                                    myCommandMonitoringUpdate = new SqlCommand(strCmdMonitoring, SQLConnectionMonitoringUpdate);
                                                    myCommandMonitoringUpdate.ExecuteNonQuery();
                                                    break;
                                                case "ReplaceRowsBasedOnDate":
                                                    strCmdMonitoring = "UPDATE tbl_ServiceTables" +
                                                  " SET SpecialConditionID = '" + strCurrentSpecialConditionID + "'" +
                                                  " WHERE TableName = '" + strTableName + "'";
                                                    WriteTolog(strCmdMonitoring, 0);
                                                    myCommandMonitoringUpdate = new SqlCommand(strCmdMonitoring, SQLConnectionMonitoringUpdate);
                                                    myCommandMonitoringUpdate.ExecuteNonQuery();
                                                    break;
                                            }

                                            //if (strSpecialCondition != "ReplaceAll")
                                            //{
                                            //    strCmdMonitoring = "UPDATE tbl_ServiceTables" +
                                            //      //" SET LastUID = " + intCurrentUniqueID.ToString() +
                                            //      " SET LastUID = '" + strCurrentUniqueID + "'" +
                                            //      " WHERE TableName = '" + strTableName + "'";
                                            //    WriteTolog(strCmdMonitoring, 0);
                                            //    myCommandMonitoringUpdate = new SqlCommand(strCmdMonitoring, SQLConnectionMonitoringUpdate);
                                            //    myCommandMonitoringUpdate.ExecuteNonQuery();
                                            //}

                                            intItemsProcessed += 1;
                                        }


                                        //WriteTolog("________________________________", 1);
                                        WriteTolog("Number of items processed: " + intItemsProcessed.ToString(), 1);
                                        WriteTolog("__________________________________________", 1);
                                    }

                                    DSData.Clear();
                                    DSData = null;
                                }
                                else
                                {
                                    string s = "Do not Conitune";
                                }
                                Application.DoEvents();
                                #endregion

                            } //while (readerGetTables.Read())
                        }

                        strTableName = "";

                        readerGetTables.Close();
                        readerGetTables.Dispose();
                        myCommandMGetTables.Dispose();
                        
                        PGSQLConnection1.Close();
                        SQLConnection1.Close();
                        WriteTolog("Reader Closed", 0);

                        //strCmdMonitoring = "UPDATE tbl_ServiceTables" +
                        //  " SET LastUID = " + intCurrentUniqueID.ToString() +
                        //  " WHERE TableName = '" + strTableName + "'";
                        //WriteTolog(strCmdMonitoring, 0);
                        //myCommandMonitoring = new SqlCommand(strCmdMonitoring, SQLConnectionMonitoring);
                        //myCommandMonitoring.ExecuteNonQuery();

                        strCmdMonitoring = "UPDATE tbl_ServiceMonitor" +
                           " SET EndDateTime = '" + System.DateTime.Now.ToString("u").Replace("Z", "") + "', Status = 4" +
                           " WHERE RunID = " + intRunID;
                        WriteTolog(strCmdMonitoring, 0);
                        myCommandMonitoring = new SqlCommand(strCmdMonitoring, SQLConnectionMonitoring);
                        myCommandMonitoring.ExecuteNonQuery();
                    }
                }
                #endregion

                SQLConnectionMonitoring.Close();
                WriteTolog("Closing SQLConnectionMonitoring", 0);
                SQLConnectionMonitoringUpdate.Close();
                WriteTolog("Closing SQLConnectionMonitoringUpdate", 0);

                RemoveOldFiles();

                WriteTolog("End of process", 1);

                WriteTolog("strTimerInterval in Minutes: " + strTimerInterval, 0);
                strTimerInterval = ConvertMinutesToMilliseconds(Convert.ToDouble(strTimerInterval)).ToString();
                WriteTolog("strTimerInterval in Milliseconds: " + strTimerInterval, 0);
                //timer1.Interval = Convert.ToInt32(strTimerInterval);
                WriteTolog("Enabling timer", 0);
                //timer1.Enabled = true;
            }
            catch (Exception ex)
            {
                //email the list of specified users

                WriteTolog("error: " + ex.Message, 1);
                WriteTolog("strTimerInterval in Minutes: " + strTimerInterval, 0);
                strTimerInterval = ConvertMinutesToMilliseconds(Convert.ToDouble(strTimerInterval)).ToString();
                WriteTolog("strTimerInterval in Milliseconds: " + strTimerInterval, 0);

                if (strTableName != "")
                {
                    //SendNotificationEmail("There was an issue on table: " + strTableName + " - \r\n\r\n" + ex.Message);
                    SendNotificationEmail("There was an issue on table: " + strTableName + " <br /><br />Error message: <br />" + ex.Message);                    
                }
                else
                {
                    SendNotificationEmail("Error message: <br />" + ex.Message);
                }

                //timer1.Interval = Convert.ToInt32(strTimerInterval);                
                //throw;
                WriteTolog("Enabling timer", 0);
                //timer1.Enabled = true;    

                //if (SQLConnectionMonitoring.State == ConnectionState.Open)
                //{
                //    //strCmdMonitoring = "UPDATE tbl_ServiceTables" +
                //    //      " SET LastUID = " + intCurrentUniqueID.ToString() +
                //    //      " WHERE TableName = '" + strTableName + "'";
                //    //WriteTolog(strCmdMonitoring, 1);
                //    //myCommandMonitoring = new SqlCommand(strCmdMonitoring, SQLConnectionMonitoring);
                //    //myCommandMonitoring.ExecuteNonQuery();

                //    strCmdMonitoring = "UPDATE tbl_ServiceMonitor" +
                //           " SET EndDateTime = '" + System.DateTime.Now.ToString("u").Replace("Z", "") + "', Status = 3" +
                //           " WHERE RunID = " + intRunID;
                //    WriteTolog(strCmdMonitoring, 0);
                //    myCommandMonitoring = new SqlCommand(strCmdMonitoring, SQLConnectionMonitoring);
                //    myCommandMonitoring.ExecuteNonQuery();

                //    SQLConnectionMonitoring.Close();
                //    WriteTolog("Closing SQLConnectionMonitoring", 1);
                //    SQLConnectionMonitoringUpdate.Close();
                //    WriteTolog("Closing SQLConnectionMonitoringUpdate", 1);
                //}

                ////timer1.Interval = Convert.ToInt32(strTimerInterval);
                WriteTolog("End of process", 1);
                
            }
        }

        private string GetSetting(string SettingName)
        {
            string strSettingValue = string.Empty;
            SqlCommand myCommandMonitoring;
            SqlConnection SQLConnectionMonitoring = new SqlConnection();

            WriteTolog("Starting GetSetting", 0);
            WriteTolog("Reading XML...", 0);
            XML_ReadFile(Application.StartupPath.ToString() + "\\SQLServerList.xml");
            WriteTolog("After XML_ReadFile", 0);
            WriteTolog("strSQLServerMonitoringDBName: " + strSQLServerMonitoringDBName, 0);
            WriteTolog("strTimerInterval: " + strTimerInterval, 0);

            string strSqlMonitoring = ("user id=" + strSQLServerMonitoringUsername + ";" +
                            "password=" + strSQLServerMonitoringPassword + ";server=" + strSQLServerMonitoringItemName + ";" +
                            //"Trusted_Connection=yes;" +
                            "database=" + strSQLServerMonitoringDBName + ";" +
                            "connection timeout=30");
            WriteTolog("strSql: " + strSqlMonitoring, 0);
            SQLConnectionMonitoring = new SqlConnection(strSqlMonitoring);
            WriteTolog("Opening SQLConnectionMonitoring Connection", 0);
            SQLConnectionMonitoring.Open();
            WriteTolog("Connection to SQLConnectionMonitoring was successfull", 0);


            string strCmdMonitoring = "SELECT SettingValue FROM tbl_ServiceSettings WHERE SettingName = '" + SettingName + "'";                           
            WriteTolog(strCmdMonitoring, 0);
            using (myCommandMonitoring = new SqlCommand(strCmdMonitoring, SQLConnectionMonitoring))
            {
                //intRunID = Convert.ToInt32(myCommandMonitoring.ExecuteScalar().ToString());
                using (SqlDataReader reader = myCommandMonitoring.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        //Console.WriteLine("{0} {1} {2}",
                        //reader.GetInt32(0), reader.GetString(1), reader.GetString(2));
                        strSettingValue =  reader.GetString(0);
                    }
                }
            }

            return strSettingValue;
        }

        private void XML_ReadFile(string FileName)
        {
            WriteTolog("Starting XML_ReadFile", 1);
            //WriteTolog(FileName, 1);
            if (File.Exists(FileName) == true)
            {
                strListOfStrings.Clear();
                XmlDocument xmlDoc = new XmlDocument();
                WriteTolog("Loading XML Doc", 0);
                xmlDoc.Load(FileName);                
                string strNodeName = string.Empty;
                //int DatagridRowCount = 0;

                try
                {
                    XmlNodeList nlXMLtem = xmlDoc.GetElementsByTagName("SQLServerItem");
                    for (int nlXMLtemID = 0; nlXMLtemID < nlXMLtem.Count; ++nlXMLtemID)
                    {
                        if (nlXMLtem[nlXMLtemID].HasChildNodes == true)
                        {
                            for (int nodeID = 0; nodeID < nlXMLtem[nlXMLtemID].ChildNodes.Count; ++nodeID)
                            {
                                strNodeName = nlXMLtem[nlXMLtemID].ChildNodes[nodeID].Name;
                                switch (strNodeName)
                                {
                                    case "PGServerItemName":
                                        strPGServerItemName = nlXMLtem[nlXMLtemID].ChildNodes[nodeID].InnerText;
                                        break;
                                    case "PGServerUsername":
                                        strPGServerUsername = nlXMLtem[nlXMLtemID].ChildNodes[nodeID].InnerText;
                                        break;
                                    case "PGServerPassword":
                                        strPGServerPassword = nlXMLtem[nlXMLtemID].ChildNodes[nodeID].InnerText;
                                        break;
                                    case "PGServerDB":
                                        strPGServerDB = nlXMLtem[nlXMLtemID].ChildNodes[nodeID].InnerText;
                                        break;
                                    case "SQLServerItemName":
                                        strSQLServerItemName = nlXMLtem[nlXMLtemID].ChildNodes[nodeID].InnerText;
                                        break;
                                    case "SQLServerUsername":
                                        strSQLServerUsername = nlXMLtem[nlXMLtemID].ChildNodes[nodeID].InnerText;
                                        break;
                                    case "SQLServerPassword":
                                        strSQLServerPassword = nlXMLtem[nlXMLtemID].ChildNodes[nodeID].InnerText;
                                        break;
                                    case "SQLServerDB":
                                        strSQLServerDBName = nlXMLtem[nlXMLtemID].ChildNodes[nodeID].InnerText;
                                        break;
                                    case "SQLServerMonitoringItemName":
                                        strSQLServerMonitoringItemName = nlXMLtem[nlXMLtemID].ChildNodes[nodeID].InnerText;
                                        break;
                                    case "SQLServerMonitoringUsername":
                                        strSQLServerMonitoringUsername = nlXMLtem[nlXMLtemID].ChildNodes[nodeID].InnerText;
                                        break;
                                    case "SQLServerMonitoringPassword":
                                        strSQLServerMonitoringPassword = nlXMLtem[nlXMLtemID].ChildNodes[nodeID].InnerText;
                                        break;
                                    case "SQLServerMonitoringDB":
                                        strSQLServerMonitoringDBName = nlXMLtem[nlXMLtemID].ChildNodes[nodeID].InnerText;
                                        break;
                                    case "TimerIntervalInMinutes":
                                        strTimerInterval = nlXMLtem[nlXMLtemID].ChildNodes[nodeID].InnerText;
                                        double dblConvertMinutesToMilliseconds = ConvertMinutesToMilliseconds(Convert.ToDouble(strTimerInterval));
                                        strTimerInterval = Convert.ToString(dblConvertMinutesToMilliseconds);
                                        break;
                                    case "WriteAllEventToTheLogFile":
                                        strWriteAllEventToTheLogFile = nlXMLtem[nlXMLtemID].ChildNodes[nodeID].InnerText;
                                        if (strWriteAllEventToTheLogFile == "Yes")
                                            blnLogEvents = true;
                                        else
                                            blnLogEvents = false;
                                        break;
                                    case "NumberOfDaysTokeepLogFiles":
                                        intNumberOfDaysTokeepLogFiles = Convert.ToInt16(nlXMLtem[nlXMLtemID].ChildNodes[nodeID].InnerText);
                                        if (intNumberOfDaysTokeepLogFiles == 0)
                                            intNumberOfDaysTokeepLogFiles = 10;
                                        break;
                                    default:
                                        break;
                                }
                            }

                            if ((strSQLServerItemName != null))
                            {
                                strListOfStrings.Add(strSQLServerItemName);
                                strListOfStringsUsers.Add(strSQLServerUsername);
                                strListOfStringsPasswords.Add(strSQLServerPassword);
                                strListOfStringsDatabases.Add(strSQLServerDBName);
                            }
                        }
                    }
                    xmlDoc = null;
                }


                catch (Exception ex)
                {
                    //string s = ex.Message;
                    WriteTolog("error: " + ex.Message, 1);
                    //throw;
                }
            }
            else
            {
                WriteTolog("File does not exist", 1);
                //Xml_CreateFile(FileName);
                //XML_ReadFile(FileName);
            }
        }

        public static double ConvertMinutesToMilliseconds(double minutes)
        {
            return TimeSpan.FromMinutes(minutes).TotalMilliseconds;
        }

        public static double ConvertMillisecondsToMinutes(double milliseconds)
        {
            return TimeSpan.FromMilliseconds(milliseconds).TotalMinutes;
        }

        public void WriteTolog(string strMessage, int intforceLog)
        {
            // intforceLog allows you to force the app to log with out it 
            // being enabled in the XML             
            try
            {
                if ((blnLogEvents == true) || (intforceLog == 1))
                {
                    int intMonth = DateTime.Now.Month;
                    string strMonth = string.Empty;
                    int intDay = DateTime.Now.Day;
                    string strDay = string.Empty;

                    if (intMonth < 10)
                    {
                        strMonth = "0" + intMonth.ToString();
                    }
                    else
                    {
                        strMonth = intMonth.ToString();
                    }

                    if (intDay < 10)
                    {
                        strDay = "0" + intDay.ToString();
                    }
                    else
                    {
                        strDay = intDay.ToString();
                    }

                    //string strDateTime = DateTime.Now.Year.ToString() + "-" + DateTime.Now.Month + "-" + DateTime.Now.Day + " " + DateTime.Now.ToLongTimeString().ToString() + " ==> ";
                    string strDateTime = DateTime.Now.Year.ToString() + "-" + strMonth + "-" + strDay + " " + DateTime.Now.ToLongTimeString().ToString() + " -> ";


                    if (strLogPath == string.Empty)
                    {
                        strLogPath = Application.StartupPath + "\\Logs";
                        //strLogPath = System.AppDomain.CurrentDomain.BaseDirectory + "\\Logs";
                        if (Directory.Exists(strLogPath) == false)
                        {
                            Directory.CreateDirectory(strLogPath);
                        }
                    }

                    StreamWriter sw = new StreamWriter(strLogPath + "\\AllLifeDC_Log-" + DateTime.Now.Year.ToString() + "-" + strMonth + "-" + strDay + ".txt", true);
                    sw.WriteLine(strDateTime + strMessage);
                    sw.Flush();
                    sw.Close();
                }
            }
            catch (Exception EX)
            {
                string strError = EX.Message;
            }
        }

        public bool isDate(string value)
        {
            DateTime date;
            return DateTime.TryParse(value, out date);
        }

        private void RemoveOldFiles()
        {
            bool boolDefaultTime = true;	//Boolean for activating default comparison duration.
            int intDays = 0;				//variables for storing user's input for Days
            int intHours = 0;				//variables for storing user's input for Hours
            int intMinutes = 0;			    //variables for storing user's input for Minutes
            int intSeconds = 0;				//variables for storing user's input for Seconds

            intDays = intNumberOfDaysTokeepLogFiles;

            boolDefaultTime = false;         //Can set up the app to not use the default time
            TimeSpan diffTSpan = new TimeSpan(30, 0, 0, 0);  //(intDays, intHours, intMinutes, intSeconds)

            DirectoryInfo dirInfo;
            try
            {
                //dirInfo = new DirectoryInfo(System.AppDomain.CurrentDomain.BaseDirectory + "\\Logs");
                dirInfo = new DirectoryInfo(Application.StartupPath.ToString() + "\\Logs");
                //Application.StartupPath.ToString() + "\\SQLServerList.xml"

                foreach (FileInfo f in dirInfo.GetFiles())
                {
                    DateTime systemDt = DateTime.Now;
                    DateTime fileDt = f.LastWriteTime;
                    DateTime cpmTime;
                    ////IF SYSTEM TIME < FILE WRITE TIME - send a warning message since anyway the file won't be deleted with the current logic
                    //if (f.LastWriteTime > systemDt)
                    //    Console.WriteLine("Some one messed the system clock or the file write time was in a different time zone!!!");

                    //USE THE DEFAULT VALUES
                    if (boolDefaultTime == true)
                    {
                        cpmTime = fileDt + diffTSpan;
                    }
                    else	//USING USER INPUTTED VALUES
                    {
                        TimeSpan customTSpan = new TimeSpan(intDays, intHours, intMinutes, intSeconds);
                        cpmTime = fileDt + customTSpan;
                    }

                    //Console.WriteLine(cpmTime.ToLongDateString());
                    //Console.WriteLine(cpmTime.ToLongTimeString());

                    //CHECKING IF THE FILE LIFE TIME IS MORE THAN THE CURRENT SYSTEM TIME. IF YES FILE IS VALID
                    if (DateTime.Compare(cpmTime, systemDt) > 0)
                        //Console.WriteLine("Still Valid!");
                        WriteTolog(f.Name + " - is still valid, will not be deleted", 0);
                    else	//CHECKING IF THE FILE LIFE TIME IS <= THE CURRENT SYSTEM TIME. IF YES - FILE IS SET FOR DELETION
                    {
                        //Console.WriteLine("{0} file is being deleted!", f.Name);
                        WriteTolog("{0} file is being deleted! " + f.Name, 0);
                        f.Delete();
                        //Console.WriteLine("{0} file has been deleted!", f.Name);
                        WriteTolog("{0} file has been deleted! " + f.Name, 0);
                    }
                }
            }
            catch (Exception ex)
            {
                WriteTolog("error: " + ex.Message, 1);
                //throw;
            }
        }

        private void SendNotificationEmail(string Emailbody)
        {
            SmtpClient smtpClient = new SmtpClient();
            MailMessage message = new MailMessage();
            string EmailDisplayName = string.Empty;
            string EmailUserName = string.Empty;
            string EmailPassword = string.Empty;
            string EmailSMTPServer = string.Empty;
            string EmailSMTPPort = string.Empty;
            string EmailEnableSsl = string.Empty;
            string EmailTo = string.Empty;
            string EmailSubject = string.Empty;

            EmailSMTPServer = GetSetting("EmailSMTPServer");
            EmailSMTPPort = GetSetting("EmailSMTPPort");
            EmailEnableSsl = GetSetting("EmailEnableSsl");

            EmailUserName = GetSetting("EmailUserNameNotification");
            EmailPassword = GetSetting("EmailPasswordNotification");
            EmailDisplayName = GetSetting("EmailDisplayNameNotification");
            EmailTo = GetSetting("EmailTo");
            EmailSubject = GetSetting("EmailSubject");

            MailAddress fromAddress = new MailAddress(EmailUserName, EmailDisplayName);
            NetworkCredential basicCredential = new NetworkCredential(EmailUserName, EmailPassword);
            smtpClient.Host = EmailSMTPServer;
            smtpClient.Port = Convert.ToInt16(EmailSMTPPort);
            smtpClient.UseDefaultCredentials = false;
            smtpClient.DeliveryMethod = SmtpDeliveryMethod.Network;
            smtpClient.Credentials = new NetworkCredential(EmailUserName, EmailPassword);
            //if (EmailUserName.Contains("gmail.com"))
            if (EmailEnableSsl == "True")
                smtpClient.EnableSsl = true;
            else
                smtpClient.EnableSsl = false;

            smtpClient.Timeout = 1500000;
            message.From = fromAddress;

            List<string> lstEmailTo = EmailTo.Split(',').ToList();
            foreach (var item in lstEmailTo)
            {
                message.To.Add(item.Trim());
            }

            message.Subject = EmailSubject.Trim();
            message.IsBodyHtml = true;

            //message.Body = "Please find the attached report.\r\n\r\nThe report name is " + strReportName + " and the report was run from " + strStartDateValue.Substring(0, 10) + " to " + strEndDateValue.Substring(0, 10);
            message.Body = Emailbody;

            //Attachment MailAttachement = new Attachment(strFilePath);
            //message.Attachments.Add(MailAttachement);

            //string PamphletPath = Server.MapPath("~/Content/");
            //string strPamphletName = GetSetting("PamphletLocation");
            //if (strPamphletName.Length > 0)
            //{
            //    PamphletPath = System.IO.Path.Combine(PamphletPath, strPamphletName);
            //    Attachment MailAttachementPamphlet = new Attachment(PamphletPath);
            //    message.Attachments.Add(MailAttachementPamphlet);
            //}

            // Send SMTP mail
            WriteTolog("Sending email...", 1);
            smtpClient.Send(message);
            WriteTolog("The email has been sent", 1);
        }
    }
}
