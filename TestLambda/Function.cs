using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Amazon.Lambda.Core;

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
            if(DateTime.TryParse(dob, out DateTime dobj))
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
                if (jobj != null)
                {
                  
                    Console.WriteLine(input.Value);
                    Employee e = jobj.ToObject<Employee>();
                    Console.WriteLine(jobj.Value<string>("emp_name"));
                    if (e != null)
                    {
                        Console.WriteLine(e.emp_doj);

                        if (!Validate(e.emp_department.ToLower(), 'd') 
                         || !Validate(e.emp_type.ToLower(), 't') 
                         || HasValidDOB(e.emp_dob)) {
                            response += "invalid department (or) DOB (or) Employee type\n";
                        }
                        else
                        {
                            
                            response += "Employee data received\n";

                        }
                    }
                    else
                    {
                        response += "The JSON object is incomplete\n";
                    }
                }
            }catch(Exception ex)
            {
                response += "Exception occured : " + ex.Message + "\n";
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

    public class Employee{
        public int emp_id { get; set; }
        public string emp_name { get; set; }
        public string emp_type {get; set;}
        public string emp_dob { get; set; }
        public string emp_doj { get; set; }
        public string emp_department { get; set; }
    }
}
