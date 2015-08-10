using System;
using System.Collections.Generic;
using System.Web;
using System.Text;
using System.IO;
using System.Data.SqlClient;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using Micron.Security;
using Micron.Data;

namespace SFUpdateJobDesc
{
    class Program
    {
        static void Main(string[] args)
        {
            com.successfactors.sfapi.SFAPIService SFAPI = new com.successfactors.sfapi.SFAPIService();
            com.successfactors.sfapi.SFCredential myCredential = null;
            com.successfactors.sfapi.LoginResult myLoginResult = null;
            com.successfactors.sfapi.SFParameter[] myParams = null;
            com.successfactors.sfapi.QueryResult myQueryResult = null;
            string sessionId = string.Empty;

            try
            {

                SqlConnection connection = GetConnection();

                // update job descriptions for US (template_id = 24= USA template_id = 25 = Singapore, template_id = 163 = Malaysia, template_id = 164 = China)
                int templateId = 24;
                int startJobReqId = GetCount(connection, "select min(job_req_id) from hrdw.d_rec_job_requisition where template_id=" + templateId + " and internal_job_desc is NULL");
                int jobCount = GetCount(connection, "select count(job_req_id) from hrdw.d_rec_job_requisition where template_id=" + templateId + " and job_req_id > " + startJobReqId);
                int batchSize = 1;
                int batches = jobCount / batchSize;

                Console.WriteLine("Start Job Req ID:{0}", startJobReqId);

                for (int batch = 0; batch < batches; batch++)
                {


                    if (!SFAPI.isValidSession())
                    {
                        SFAPI.Url = "https://api4.successfactors.com/sfapi/v1/soap";
                        SFAPI.CookieContainer = new System.Net.CookieContainer();

                        myCredential = new com.successfactors.sfapi.SFCredential();
                        myCredential.companyId = "micron";
                        myCredential.username = "HR";
                        myCredential.password = "micron@admin";

                        myLoginResult = SFAPI.login(myCredential, myParams);

                        if (myLoginResult.error == null)
                        {
                            // success!
                            sessionId = myLoginResult.sessionId;
                        }
                        else
                            return;
                    }

                    // grap the next batch of job IDs from the Micron HRDW
                    List<int> jobIds = GetJobIds(connection, startJobReqId, batchSize, templateId);

                    string jobIdFilter = "(" + string.Join(",", jobIds.ToArray()) + ")";

                    Console.WriteLine("Updating Job Req ID:{0}", jobIdFilter);

                    // build SFAPI filter expression to query a batch of job reqs between a start-end range
                    string SFQL = "SELECT id, listingLayout, extListingLayout FROM JobRequisition$" + templateId + " WHERE id in " + jobIdFilter;

                    myParams = new com.successfactors.sfapi.SFParameter[0];
                    myQueryResult = SFAPI.query(SFQL, myParams);

                    if (myQueryResult.numResults > 0)
                    {
                        foreach (com.successfactors.sfapi.SFObject myObject in myQueryResult.sfobject)
                        {
                            string reqId = myObject.Item.ToString();  // item[0] is always the ID
                            int jobReqId = Int32.Parse(reqId.Substring(reqId.IndexOf('-') + 1));
                            string intJobDesc = myObject.Any[0].InnerText;
                            string extJobDesc = myObject.Any[1].InnerText;

                            UpdateJobDesc(connection, jobReqId, intJobDesc, extJobDesc);
                        }
                    }
                    startJobReqId = jobIds[batchSize - 1];
                }

            }
            catch (Exception e)
            {
                Console.WriteLine("Exception occurred:{0}", e.Message);
            }
            Console.ReadKey();
        }


        private static Micron.Data.Credential GetCredential()
        { 
            Micron.Application.Context context = null;

            context = new Micron.Application.Context("IW ETL", "HUMAN_RESOURCES", Micron.Application.Context.Environments.Production);
            Micron.Data.Credential credential = new Credential(context, "HRLOADER");

            return (credential);
        }

        private static SqlConnection GetConnection()
        {
            Micron.Data.Credential credential = GetCredential();
            SqlConnection connection = null;

            System.Data.SqlClient.SqlConnectionStringBuilder builder = new System.Data.SqlClient.SqlConnectionStringBuilder();
            builder["Data Source"] = credential.Domain;
            builder["integrated Security"] = false;
            builder.UserID = credential.UserID;
            builder.Password = credential.Password;
            builder["Initial Catalog"] = "hr";

            try
            {
                connection = new SqlConnection(builder.ConnectionString);
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception occurred:{0}", e.Message);
            }

            return connection;
        }

        private static int GetCount(SqlConnection connection, string sql)
        {
            int result = 0;

            try
            {
                SqlCommand command = new SqlCommand(sql, connection);

                if (connection.State != System.Data.ConnectionState.Open)
                    connection.Open();

                int? value = (int?)command.ExecuteScalar();
                if (value.HasValue)
                {
                    result = (int)value;
                }
                connection.Close();
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception occurred:{0}", e.Message);
            }
            return (result);
        }

        private static List<int> GetJobIds(SqlConnection connection, int startId, int count, int templateId)
        {
            List<int> result = new List<int>();

            try
            {
                string sql = "SELECT top " + count + " job_req_id FROM hrdw.d_rec_job_requisition WHERE template_id=" + templateId + " AND job_req_id >= " + startId + " ORDER BY job_req_id";
                SqlCommand command = new SqlCommand(sql, connection);

                if (connection.State != System.Data.ConnectionState.Open)
                    connection.Open();

                using (SqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        result.Add(reader.GetInt32(0));
                    }
                }

                connection.Close();
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception occurred:{0}", e.Message);
            }
            
            return (result);
        }


        private static void UpdateJobDesc(SqlConnection connection, int jobReqId, string intDesc, string extDesc)
        {
            // Do dummy update if no job description is provided
            if (intDesc.Length == 0)
                intDesc = "No job description available.";

            if (extDesc.Length == 0)
                extDesc = "No job description available.";

            // Escape single quotes
            intDesc = intDesc.Replace("'", "''");
            extDesc = extDesc.Replace("'", "''");

            string sql = "UPDATE hrdw.d_rec_job_requisition SET internal_job_desc = '" + intDesc + "', external_job_desc = '" + extDesc + "' WHERE job_req_id =" + jobReqId;

            try
            {
                SqlCommand command = new SqlCommand(sql, connection);

                if (connection.State != System.Data.ConnectionState.Open)
                    connection.Open();

                command.ExecuteNonQuery();
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception occurred:{0}", e.Message);
            }
        }
    }
}
