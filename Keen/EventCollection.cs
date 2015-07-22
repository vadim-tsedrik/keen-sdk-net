using System.Net;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;

namespace Keen.Core
{
    /// <summary>
    /// EventCollection implements the IEventCollection interface which represents the Keen.IO EventCollection API methods.
    /// </summary>
    internal class EventCollection : IEventCollection
    {
        private string _serverUrl;
        private IProjectSettings _prjSettings;

        public async System.Threading.Tasks.Task<JObject> GetSchema(string collection)
        {
            using (var client = new HttpClient())
            {
                client.DefaultRequestHeaders.Add("Authorization", _prjSettings.MasterKey);
                var responseMsg = await client.GetAsync(_serverUrl + collection)
                    .ConfigureAwait(continueOnCapturedContext: false);
                var responseString = await responseMsg.Content.ReadAsStringAsync()
                    .ConfigureAwait(continueOnCapturedContext: false);
                var response = JObject.Parse(responseString);

                // error checking, throw an exception with information from the json 
                // response if available, then check the HTTP response.
                KeenUtil.CheckApiErrorCode(response);
                if (!responseMsg.IsSuccessStatusCode)
                    throw new KeenException("GetSchema failed with status: " + responseMsg.StatusCode);

                return response;
            }
        }

        public async System.Threading.Tasks.Task DeleteCollection(string collection)
        {
            using (var client = new HttpClient())
            {
                client.DefaultRequestHeaders.Add("Authorization", _prjSettings.MasterKey);
                var responseMsg = await client.DeleteAsync(_serverUrl + collection)
                    .ConfigureAwait(continueOnCapturedContext: false); 
                if (!responseMsg.IsSuccessStatusCode)
                    throw new KeenException("DeleteCollection failed with status: " + responseMsg.StatusCode);
            }
        }

        public async System.Threading.Tasks.Task AddEvent(string collection, JObject anEvent)
        {
            var content = anEvent.ToString();

            using (var client = new HttpClient())
            using (var contentStream = new StreamContent(new MemoryStream(Encoding.UTF8.GetBytes(content))))
            {
                contentStream.Headers.Add("content-type", "application/json");

                client.DefaultRequestHeaders.Add("Authorization", _prjSettings.WriteKey);
                string responseString= null;
                HttpResponseMessage httpResponse = null;
                try
                {
                    httpResponse = await client.PostAsync(_serverUrl + collection, contentStream)
                        .ConfigureAwait(continueOnCapturedContext: false);
                    responseString = await httpResponse.Content.ReadAsStringAsync()
                        .ConfigureAwait(continueOnCapturedContext: false);
                }
                catch (Exception)
                {
                    //throw new KeenException("AddEvent failed. Api is not available now.");
                }

                JObject jsonResponse = null;
                try
                {
                    // Normally the response content should be parsable JSON,
                    // but if the server returned a 404 error page or something
                    // like that, this will throw. 
                    if (!string.IsNullOrWhiteSpace(responseString))
                        jsonResponse = JObject.Parse(responseString);
                }
                catch (Exception) 
                { }

                // error checking, throw an exception with information from the 
                // json response if available, then check the HTTP response.
                KeenUtil.CheckApiErrorCode(jsonResponse);
                if (httpResponse != null && httpResponse.StatusCode != HttpStatusCode.NotFound && !httpResponse.IsSuccessStatusCode)
                    throw new KeenException("AddEvent failed with status: " + httpResponse);
            }
        }

        public EventCollection(IProjectSettings prjSettings)
        {
            _prjSettings = prjSettings;

            _serverUrl = string.Format("{0}projects/{1}/{2}/",
                _prjSettings.KeenUrl, _prjSettings.ProjectId, KeenConstants.EventsResource);
        }
    }
}
