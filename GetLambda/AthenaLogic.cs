
using System;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using Amazon;
using Amazon.Athena;
using Amazon.Athena.Model;
using Newtonsoft.Json.Linq;

namespace GetLambda
{
    class AthenaLogic
    {
        private const String ATHENA_TEMP_PATH = "s3://prithivienvbucket/";
        private const String ATHENA_DB = "anotherdatabase";


        public static async Task<JRaw> QueryAthenaAndSend(string datestring)
        {
            using (var client = new AmazonAthenaClient(Amazon.RegionEndpoint.USEast2))
            {
                QueryExecutionContext qContext = new QueryExecutionContext();
                qContext.Database = ATHENA_DB;
                ResultConfiguration resConf = new ResultConfiguration();
                resConf.OutputLocation = ATHENA_TEMP_PATH;

                Console.WriteLine("Created Athena Client");
                List<Dictionary<String, String>> items = await run(client, qContext, resConf,datestring);
               // return items.ToString();
                JObject obj = new JObject();
                if (items.Count == 0)
                {
                    obj.Add("count", "zero");
                    return obj.ToObject<JRaw>();
                }
                obj.Add("Count", items.Count);
                for (int i = 1; i < items.Count; i++)
                {
                    JProperty nameProp = null;
                    JProperty idProp = null;
                    foreach (KeyValuePair<String, String> pair in items[i])
                    {

                        if (pair.Key == "emp_name")
                            nameProp = new JProperty("name", pair.Value);
                        if (pair.Key == "emp_id")
                            idProp = new JProperty("id", pair.Value);
                    }
                    if (nameProp != null && idProp != null)
                        obj.Add("emp " + i, new JObject(nameProp, idProp));
                    else
                        return null;
                }
                return obj.ToObject<JRaw>();

            }
        }

        async static Task<List<Dictionary<String, String>>> run(IAmazonAthena client, QueryExecutionContext qContext, ResultConfiguration resConf,string datestring)
        {
            /* Execute a simple query on a table */
            StartQueryExecutionRequest qReq = new StartQueryExecutionRequest()
            {
                QueryString = $@"SELECT * FROM emptable where emp_dob = '{datestring}'",
                QueryExecutionContext = qContext,
                ResultConfiguration = resConf
            };

            try
            {
                /* Executes the query in an async manner */
                StartQueryExecutionResponse qRes = await client.StartQueryExecutionAsync(qReq);
                /* Call internal method to parse the results and return a list of key/value dictionaries */
                List<Dictionary<String, String>> items = await getQueryExecution(client, qRes.QueryExecutionId);
                return items;
            }
            catch (InvalidRequestException e)
            {
                Console.WriteLine("Run Error: {0}", e.Message);
            }
            return null;
        }
        async static Task<List<Dictionary<String, String>>> getQueryExecution(IAmazonAthena client, String id)
        {
            List<Dictionary<String, String>> items = new List<Dictionary<String, String>>();
            GetQueryExecutionResponse results = null;
            QueryExecution q = null;
            /* Declare query execution request object */
            GetQueryExecutionRequest qReq = new GetQueryExecutionRequest()
            {
                QueryExecutionId = id
            };
            /* Poll API to determine when the query completed */
            do
            {
                List<Dictionary<string, string>> lists = new List<Dictionary<string, string>>();
                try
                {
                    results = await client.GetQueryExecutionAsync(qReq);
                    
                    if (results == null)
                    {
                        Dictionary<string, string> dic = new Dictionary<string, string>();
                        dic.Add("error","results is null");
                        lists.Add(dic);
                        return lists;
                    }
                    

                    
                    q = results.QueryExecution;

                    if (q == null)
                    {
                        Dictionary<string, string> dic = new Dictionary<string, string>();
                        dic.Add("error", "q is null");
                        lists.Add(dic);
                        return lists;
                    }

                    Console.WriteLine("Status: {0}... {1}", q.Status.State, q.Status.StateChangeReason);

                    await Task.Delay(5000); //Wait for 5sec before polling again
                }
                catch (InvalidRequestException e)
                {
                    if (results == null)
                    {
                        Dictionary<string, string> dic = new Dictionary<string, string>();
                        dic.Add("error", "Invalid request exception happened");
                        lists.Add(dic);
                        return lists;
                    }
                    Console.WriteLine("GetQueryExec Error: {0}", e.Message);
                }
            } while (q.Status.State == "RUNNING" || q.Status.State == "QUEUED");

            Console.WriteLine("Data Scanned for {0}: {1} Bytes", id, q.Statistics.DataScannedInBytes);

            /* Declare query results request object */
            GetQueryResultsRequest resReq = new GetQueryResultsRequest()
            {
                QueryExecutionId = id,
                MaxResults = 10
            };

            GetQueryResultsResponse resResp = null;
            /* Page through results and request additional pages if available */
            do
            {
                resResp = await client.GetQueryResultsAsync(resReq);
                /* Loop over result set and create a dictionary with column name for key and data for value */
                foreach (Row row in resResp.ResultSet.Rows)
                {
                    Dictionary<String, String> dict = new Dictionary<String, String>();
                    for (var i = 0; i < resResp.ResultSet.ResultSetMetadata.ColumnInfo.Count; i++)
                    {
                        dict.Add(resResp.ResultSet.ResultSetMetadata.ColumnInfo[i].Name, row.Data[i].VarCharValue);
                    }
                    items.Add(dict);
                }

                if (resResp.NextToken != null)
                {
                    resReq.NextToken = resResp.NextToken;
                }
            } while (resResp.NextToken != null);

            /* Return List of dictionary per row containing column name and value */
            return items;
        }
    }
}

