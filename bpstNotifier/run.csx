#r "System.Data"

using System;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Net.Http;
using System.Text;


public static void Run(TimerInfo myTimer, TraceWriter log)
{
    var connectionString = System.Configuration.ConfigurationManager.ConnectionStrings["resourceToMonitor"].ToString();
    var deploymentId = System.Configuration.ConfigurationManager.ConnectionStrings["deploymentId"].ToString();
    var emails = System.Configuration.ConfigurationManager.ConnectionStrings["emails"].ToString();
    var notifierUrl = System.Configuration.ConfigurationManager.ConnectionStrings["notifierConnectionString"].ToString();
    var sprocWithSchema = System.Configuration.ConfigurationManager.ConnectionStrings["sprocWithSchema"].ToString();
    var templateName = System.Configuration.ConfigurationManager.ConnectionStrings["templateName"].ToString();
    
    var payload = "{" +
                       $"\"deploymentId\":\"{deploymentId}\"," +
                       $"\"to\":\"{emails}\"," +
                       $"\"templateName\":\"{templateName}\"" +
                   "}";

    var status = ExecuteCountsProcedure(connectionString, sprocWithSchema.Split('.')[0], sprocWithSchema.Split('.')[1]);

    if (status)
       {
            HttpClient client = new HttpClient();
            client.BaseAddress = new Uri(notifierUrl);

            var resp = client.PostAsync(notifierUrl,new StringContent(payload, Encoding.UTF8, "application/json")).Result;
       }
       else
       {
            Debug.WriteLine("DataPull not complete");
       }
}


public static bool ExecuteCountsProcedure(string connectionString, string schema, string spName)
{
    using (SqlConnection conn = new SqlConnection(connectionString))
    {
        using (SqlCommand command = new SqlCommand(schema + "." + spName, conn)
        {
           CommandType = CommandType.StoredProcedure
        })
        {
            conn.Open();
            var reader = command.ExecuteReader();
            var table = new DataTable();
            table.Load(reader);

            var values = table.Columns["EntityName"];
            foreach (DataRow row in table.Rows)
            {
                if (((Int64)row["Count"]) > 0)
                    {
                        return true;
                    }
            }
            return false;
         }
    }
}