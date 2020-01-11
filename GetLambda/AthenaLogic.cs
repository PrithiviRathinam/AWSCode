
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
        private const String ATHENA_DB = "empdb";
        public static async Task<JRaw> QueryAthenaAndSend(string datestring)
        {
            using (var client = new AmazonAthenaClient(Amazon.RegionEndpoint.USEast2))
            {
                QueryExecutionContext qContext = new QueryExecutionContext();
                qContext.Database = ATHENA_DB;
                ResultConfiguration resConf = new ResultConfiguration();
                resConf.OutputLocation = ATHENA_TEMP_PATH;

                Console.WriteLine("Created Athena Client");
                List<Dictionary<String, String>> items = await Run(client, qContext, resConf,datestring);
                
                if(items.Count == 1 && items[0].ContainsKey("error"))
                {
                    items[0].TryGetValue("error", out string errorinfo);
                    return new JRaw(errorinfo);
                }
                // return items.ToString();
                JObject obj = new JObject();
                if (items.Count == 0)
                {
                    obj.Add("count", "zero");
                    return obj.ToObject<JRaw>();
                }
                obj.Add("count", items.Count);
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

        async static Task<List<Dictionary<String, String>>> Run(IAmazonAthena client, QueryExecutionContext qContext, ResultConfiguration resConf,string datestring)
        {
            /* Execute a simple query on a table */

            StartQueryExecutionRequest qReq = new StartQueryExecutionRequest()
            {
                QueryString = $@"SELECT * FROM emptable where emp_dob = '{datestring}'",
                QueryExecutionContext = qContext,
                ResultConfiguration = resConf
            };
            List<Dictionary<String, String>> items = null;
            try
            {
                /* Executes the query in an async manner */
                StartQueryExecutionResponse qRes = await client.StartQueryExecutionAsync(qReq);
                /* Call internal method to parse the results and return a list of key/value dictionaries */
                 items = await getQueryExecution(client, qRes.QueryExecutionId);
                if(items == null)
                {
                    Dictionary<string, string> dic1 = new Dictionary<string, string>();
                    dic1.Add("error", "items from query execution is empty");
                    items.Add(dic1);  
                }
                
                
            }
            catch (InvalidRequestException e)
            {
                Dictionary<string, string> dic1 = new Dictionary<string, string>();
                dic1.Add("error", "exception at " + " (Run method) " + " : " + e.Message);
                if (items == null)
                    items = new List<Dictionary<string, string>>();
                items.Add(dic1);
                Console.WriteLine("Run Error: {0}", e.Message);
            }
            
            return items;
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
                        Dictionary<string, string> dic1 = new Dictionary<string, string>();
                        dic1.Add("error","results is null");
                        lists.Add(dic1);
                        return lists;
                    }
 
                    q = results.QueryExecution;

                    if (q == null)
                    {
                        Dictionary<string, string> dic3 = new Dictionary<string, string>();
                        dic3.Add("error", "q is null");
                        lists.Add(dic3);
                        return lists;
                    }

                    Console.WriteLine("Status: {0}... {1}", q.Status.State, q.Status.StateChangeReason);

                    await Task.Delay(5000); //Wait for 5sec before polling again
                }
                catch (InvalidRequestException e)
                {
                   
                        Dictionary<string, string> dic2 = new Dictionary<string, string>();
                        dic2.Add("error", "exception : " + " (Run method) " + " : " + e.Message);
                        lists.Add(dic2);
                    Console.WriteLine("GetQueryExec Error: {0}", e.Message);

                    return lists;
                  
                }
            } while (q.Status.State == "RUNNING" || q.Status.State == "QUEUED");

            Console.WriteLine("Data Scanned for {0}: {1} Bytes", id, q.Statistics.DataScannedInBytes);

            /* Declare query results request object */
            GetQueryResultsRequest resReq = new GetQueryResultsRequest()
            {
                QueryExecutionId = id,
                MaxResults = 20
            };

            GetQueryResultsResponse resResp = null;
            /* Page through results and request additional pages if available */
            Dictionary<String, String> dic = new Dictionary<String, String>();
            List<Dictionary<String, String>> l = new List<Dictionary<String, String>>();
            do
            {
                resResp = await client.GetQueryResultsAsync(resReq);
                
                
                //l.Add(dict);

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
            if (items == null)
            {
                dic.Add("error","items are null here");
                l.Add(dic);
                return l;
            }
                /* Return List of dictionary per row containing column name and value */
                return items;
        }
    }
}

