using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

using Amazon.Lambda.Core;
using Newtonsoft.Json.Linq;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace GetLambda
{
    public class Function
    {

        
        /// <summary>
        /// A  function that takes a date string and return the json data of employees with same dob
        /// </summary>
        /// <param name="input"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task<JRaw> FunctionHandler(string input, ILambdaContext context)
        {
            string s = "[0-9]{2}-[0-9]{2}-[0-9]{4}";
            Regex r = new Regex(s);
            if (r.IsMatch(input))
            {
                JRaw jsonresponse = await AthenaLogic.QueryAthenaAndSend(input);
                return jsonresponse;
            }
            return new JRaw("invalid date string");            
        }
    }
}
