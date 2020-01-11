using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Amazon.Lambda.Core;
using System.Globalization;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using Amazon;   
using Amazon.Athena;
using Amazon.Athena.Model;
using System.IO;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace TestLambda
{
    public class Function
    {
        //enum Department :byte
        //{
        //    FINANCE, HR, IT, ADMINISTRATION
        //}

        //enum EmpType : byte
        //{
        //    FULLTIME,INTERN,CONTRACTOR
        //}

        public static bool Validate(string tovalidate, char t)
        {
            if (t == 'd') {
                switch (tovalidate) {
                    case "hr":
                    case "administration":
                    case "it":
                    case "finance": 
                        return true;              
                }
            }
            else
            {
                switch (tovalidate)
                {
                    case "fulltime":
                    case "intern":
                    case "contractor":
                        return true;
                }
            }
            return false;
        }

        public static bool HasValidDOB(string dob)
        {
            string so = dob.Replace("-", "/");
            Console.WriteLine(DateTime.Now.Date);
            if(DateTime.TryParseExact(dob, "dd-MM-yyyy", new CultureInfo("en-US"),
                                 DateTimeStyles.None, out DateTime dobj))
            {
                if (dobj.Date < DateTime.Now.Date)
                    return true; 
            }
            return false;
        }

        /// <summary>   
        /// A simple function that takes a string and does a ToUpper
        /// </summary>
        /// <param name="input"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public string FunctionHandler(JRaw input, ILambdaContext context)
        {
            Console.WriteLine(input.ToString());
            string response = "";
            try
            {
                JToken jobj = JContainer.Parse(input.ToString());
                
                // this is not working 
                // string bucket = Environment.GetEnvironmentVariable("bucketname");

                if (jobj != null)
                {
                  
                    Console.WriteLine(input.Value);
                    MessageJSONProp e = jobj.ToObject<MessageJSONProp>();
                    Console.WriteLine(jobj.Value<string>("emp_name"));
                    if (e != null)
                    {
                        Console.WriteLine(e.emp_doj);

                        if (!Validate(e.emp_department.ToLower(), 'd') 
                         || !Validate(e.emp_type.ToLower(), 't') 
                         || !HasValidDOB(e.emp_dob)) {
                            response += "invalid department (or) DOB (or) Employee type. ";
                        }
                        else
                        {
                            string mesFile = @"C:\JsonData\MessageFile_" 
                                               + e.emp_id
                                               + ".json";
                            File.WriteAllText(mesFile, input.ToString());
                            
                            CustomS3AccessPoint.UploadMessage(mesFile);
                            response += "Employee data sent to bucket. ";

                        }
                    }
                    else
                    {
                        response += "The JSON object is incomplete. ";
                    }
                }
            }catch(Exception ex)
            {
                response += "Exception occured : " + ex.Message + ". ";
            }
            //Test JSON data :
            //{
            //    "emp_id": 1234,
            //               "emp_name": "John"
            //               "emp_type": "Fulltime",
            //               "emp_dob": "12-10-1990",
            //               "emp_doj": "10-01-2001",
            //               "emp_department": "Finance"
            //           }

            //   string s = (string)details["emp_name"];
            return response;
           
        }
    }

    public class MessageJSONProp{
        public int emp_id { get; set; }
        public string emp_name { get; set; }
        public string emp_type {get; set;}
        public string emp_dob { get; set; }
        public string emp_doj { get; set; }
        public string emp_department { get; set; }
    }

    



        class CustomS3AccessPoint
        {
            private static string bucketName = "prithivienvbucket";

            
            //private const string keyName = "message_object";
            //private string filePath;
            // Specify your bucket region (an example region is shown).
            private static RegionEndpoint bucketRegion = RegionEndpoint.USEast2;
            private static IAmazonS3 s3Client;

            public static void UploadMessage(string msgFile)
            {
                
                s3Client = new AmazonS3Client(bucketRegion);
                
                UploadFileAsync(msgFile).Wait();
            }

            private static async Task UploadFileAsync(string msgFile)
            {
                try
                {
                    var fileTransferUtility =
                        new TransferUtility(s3Client);
                    
                    // Option 1. Upload a file. The file name is used as the object key name.
                    await fileTransferUtility.UploadAsync(msgFile, bucketName);
                    Console.WriteLine("Upload Complete");
                File.Delete(msgFile);

                }
                catch (AmazonS3Exception e)
                {
                    Console.WriteLine("Error encountered on server. Message:'{0}' when writing an object", e.Message);
                }
                catch (Exception e)
                {
                    Console.WriteLine("Unknown encountered on server. Message:'{0}' when writing an object", e.Message);
                }

            }
        }
    }


