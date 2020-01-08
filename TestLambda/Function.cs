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
        
        /// <summary>   
        /// A simple function that takes a string and does a ToUpper
        /// </summary>
        /// <param name="input"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public string FunctionHandler(string input, ILambdaContext context)
        {
            Console.WriteLine(input);
            var details = JObject.Parse(input);
//            {
// “emp_id”: 1234,
// "emp_name": "John"
// “emp_type”: "Fulltime",
// “emp_dob”: "12-10-1990",
// “emp_doj”: "10-01-2001",
// “emp_department”: "Finance"
// }
           
            string s = (string)details["emp_name"];
            return s?.ToUpper();
           
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
