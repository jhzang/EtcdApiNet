using System;
using System.IO;
using System.Text;
using System.Net;
using System.Collections.Generic;
using System.Collections.Specialized;

namespace EtcdApiNet
{
    public class EtcdError
    {
        public long? errorCode = null;
        public long? index = null;
        public string message = null;
        public string cause = null;

        public override string ToString()
        {
            StringBuilder builder = new StringBuilder();
            builder.AppendFormat("\"errorCode\": {0}, \"index\": {1}, \"message\": \"{2}\"", this.errorCode, this.index, this.message);
            if (this.cause != null)
            {
                builder.AppendFormat(", \"cause\": \"{0}\"", this.cause);
            }
            return builder.ToString();
        }

        public static EtcdError Parse(SimpleJson.JsonObject jsonObj)
        {
            EtcdError error = new EtcdError();
            error.errorCode = (long)jsonObj["errorCode"];
            error.index = (long)jsonObj["index"];
            if (jsonObj.ContainsKey("cause"))
            {
                error.cause = (string)jsonObj["cause"];
            }
            error.message = (string)jsonObj["message"];
            return error;
        }
    }

    public class EtcdResultNode
    {
        private string key = null;
        private string value = null;
        private bool? dir = null;
        private long? createdIndex = null;
        private long? modifiedIndex = null;
        private long? ttl = null;
        private DateTime? expiration = null;
        private EtcdResultNode[] nodes = null;

        public override string ToString()
        {
            StringBuilder builder = new StringBuilder();
            builder.AppendFormat("{{\"key\": \"{0}\"", this.key);
            if (this.value != null)
            {
                builder.AppendFormat(", \"value\"=\"{0}\"", this.value);
            }
            if (this.dir != null)
            {
                builder.AppendFormat(", dir={0}", this.dir == true ? "true" : "false");
            }
            builder.AppendFormat(", \"modifiedIndex\": {0}, \"createdIndex\": {1}", this.modifiedIndex, this.createdIndex);
            if (this.ttl != null)
            {
                builder.AppendFormat(", ttl={0}", this.ttl.ToString());
            }
            if (this.expiration != null)
            {
                builder.AppendFormat(", expiration=\"{0}\"", this.expiration.ToString());
            }
            if (this.nodes != null && this.nodes.Length > 0)
            {
                builder.Append(", \"nodes\": [");
                List<string> nodeStrList = new List<string>();
                foreach (EtcdResultNode node in this.nodes)
                {
                    nodeStrList.Add(node.ToString());
                }
                builder.Append(string.Join(", ", nodeStrList.ToArray()));
                builder.Append("]");
            }

            builder.Append("}");
            return builder.ToString();
        }

        public static EtcdResultNode Parse(SimpleJson.JsonObject jsonObj)
        {
            EtcdResultNode node = new EtcdResultNode();
            node.key = (string)jsonObj["key"];
            node.createdIndex = (long)jsonObj["createdIndex"];
            node.modifiedIndex = (long)jsonObj["modifiedIndex"];
            if (jsonObj.ContainsKey("dir"))
            {
                node.dir = (bool)jsonObj["dir"];
            }
            if (jsonObj.ContainsKey("ttl"))
            {
                node.ttl = (long)jsonObj["ttl"];
            }
            if (jsonObj.ContainsKey("expiration"))
            {
                DateTime expiration;
                if (DateTime.TryParse((string)jsonObj["expiration"], out expiration))
                {
                    node.expiration = expiration;
                }
            }
            if (jsonObj.ContainsKey("value"))
            {
                node.value = (string)jsonObj["value"];
            }

            if (jsonObj.ContainsKey("nodes"))
            {
                SimpleJson.JsonArray children = (SimpleJson.JsonArray)jsonObj["nodes"];
                node.nodes = new EtcdResultNode[children.Count];
                for (int i = 0; i < children.Count; ++i)
                {
                    node.nodes[i] = Parse((SimpleJson.JsonObject)children[i]);
                }
            }
            return node;
        }
    }

    public class EtcdResult
    {
        public EtcdError error = null;
        public string action = "unknown";
        public EtcdResultNode node = null;
        public EtcdResultNode prevNode = null;

        public bool Successful { get { return error == null; } }

        public override string ToString()
        {

            if (this.error != null)
            {
                return string.Format("{{{0}}}", this.error.ToString());
            }
            else
            {
                StringBuilder builder = new StringBuilder();
                builder.AppendFormat("{{\"action\": \"{0}\", \"node\": {1}", this.action, this.node.ToString());
                if (this.prevNode != null)
                {
                    builder.AppendFormat(", \"prevNode\": {0}", this.prevNode.ToString());
                }
                builder.Append("}");
                return builder.ToString();
            }
        }

        public static EtcdResult Parse(string jsonStr)
        {
            Object obj;
            if (!SimpleJson.SimpleJson.TryDeserializeObject(jsonStr, out obj))
            {
                return null;
            }
            SimpleJson.JsonObject jsonObj = (SimpleJson.JsonObject)obj;
            EtcdResult result = new EtcdResult();
            if (jsonObj.ContainsKey("errorCode"))
            {
                result.action = "unknown";
                result.error = EtcdError.Parse(jsonObj);
            }
            else
            {
                if (jsonObj.ContainsKey("action"))
                {
                    result.action = (string)jsonObj["action"];
                }
                if (jsonObj.ContainsKey("node"))
                {
                    result.node = EtcdResultNode.Parse((SimpleJson.JsonObject)jsonObj["node"]);
                }
                if (jsonObj.ContainsKey("prevNode"))
                {
                    result.prevNode = EtcdResultNode.Parse((SimpleJson.JsonObject)jsonObj["prevNode"]);
                }
            }
            return result;
        }
    }

    public struct EtcdServer
    {
        public string host;
        public ushort port;
    }

    public class EtcdSession
    {
        private EtcdServer[] serverList = null;

        #region Private methods

        private static string BuildRequestUri(string host, ushort port, string prefix, string key, params string[] query)
        {
            UriBuilder builder = new UriBuilder("http", host, port);
            builder.Path = string.Format("v2/{0}{1}", prefix, key);
            if (query.Length > 0)
            {
                builder.Query = string.Join("&", query);
            }
            return builder.ToString();
        }

        private static NameValueCollection BuildRequestData(bool? dir, string value, long? ttl)
        {
            NameValueCollection data = new NameValueCollection();
            if (dir != null && dir == true)
            {
                data["dir"] = "true";
            }
            if (value != null)
            {
                data["value"] = value;
            }
            data["ttl"] = ((ttl != null && ttl > 0) ? ttl.ToString() : "");
            return data;
        }

        private WebExceptionStatus DoWebRequest(string uri, string method, out string responseStr)
        {
            Console.WriteLine("DoWebRequest: uri=\"{0}\", method=\"{1}\"", uri, method);

            responseStr = "";
            WebExceptionStatus status = WebExceptionStatus.Success;
            Stream responseStream = null;
            try
            {
                WebClient client = new WebClient();
                if (method == "GET")
                {
                    responseStream = client.OpenRead(uri);
                    StreamReader reader = new StreamReader(responseStream);
                    responseStr = reader.ReadToEnd();
                    responseStream.Close();
                    reader.Close();
                }
                else
                {
                    byte[] responseData = client.UploadValues(uri, method, new NameValueCollection());
                    responseStr = System.Text.Encoding.UTF8.GetString(responseData);
                }
            }
            catch (WebException e)
            {
                status = e.Status;
                if (e.Response != null)
                {
                    responseStream = e.Response.GetResponseStream();
                    StreamReader reader = new StreamReader(responseStream);
                    responseStr = reader.ReadToEnd();
                    responseStream.Close();
                    reader.Close();
                }
            }

            return status;
        }

        private WebExceptionStatus DoWebRequest(string uri, string method, NameValueCollection data, out string responseStr)
        {
            Console.WriteLine("DoWebRequest: uri=\"{0}\", method=\"{1}\", data=\"{2}\"", uri, method, data.ToString());

            responseStr = "";
            WebExceptionStatus status = WebExceptionStatus.Success;
            try
            {
                WebClient client = new WebClient();
                byte[] responseData = client.UploadValues(uri, method, data);
                responseStr = System.Text.Encoding.UTF8.GetString(responseData);
            }
            catch (WebException e)
            {
                status = e.Status;
                if (e.Response != null)
                {
                    Stream responseStream = e.Response.GetResponseStream();
                    StreamReader reader = new StreamReader(responseStream);
                    responseStr = reader.ReadToEnd();
                    responseStream.Close();
                    reader.Close();
                }
            }

            return status;
        }

        #endregion

        public EtcdSession(params EtcdServer[] serverList)
        {
            this.serverList = serverList;
        }

        public EtcdSession(string serverListStr)
        {
            List<EtcdServer> tmpServerList = new List<EtcdServer>();
            string[] servers = serverListStr.Split("\n\r\t ,;".ToCharArray(), StringSplitOptions.RemoveEmptyEntries);
            foreach (string serverStr in servers)
            {
                int i = serverStr.IndexOf(":");
                if (i > 0)
                {
                    EtcdServer srv = new EtcdServer();
                    srv.host = serverStr.Substring(0, i);
                    srv.port = ushort.Parse(serverStr.Substring(i + 1));
                    tmpServerList.Add(srv);
                }
            }
            this.serverList = tmpServerList.ToArray();
        }

        public EtcdResult Get(string key, bool recursive, bool? sorted = null)
        {
            List<string> query = new List<string>();
            if (recursive)
            {
                query.Add("recursive=true");
            }
            if (sorted != null && sorted == true)
            {
                query.Add("sorted=true");
            }

            foreach (EtcdServer srv in serverList)
            {
                string uri = BuildRequestUri(srv.host, srv.port, "keys", key, query.ToArray());
                string responseStr = null;
                WebExceptionStatus status = DoWebRequest(uri, "GET", out responseStr);
                if (status == WebExceptionStatus.Success || status == WebExceptionStatus.ProtocolError)
                {
                    return EtcdResult.Parse(responseStr);
                }
            }
            return null;
        }

        public EtcdResult SetValue(string key, string value, long? ttl = null, string prevValue = null, long? prevIndex = null, bool? prevExist = null)
        {
            List<string> query = new List<string>();
            if (prevValue != null)
            {
                query.Add("prevValue=" + WebUtility.UrlEncode(prevValue));
            }
            if (prevIndex != null)
            {
                query.Add("prevIndex=" + prevIndex.ToString());
            }
            if (prevExist != null && prevExist == true)
            {
                query.Add("prevExist=true");
            }
            foreach (EtcdServer srv in serverList)
            {
                string uri = BuildRequestUri(srv.host, srv.port, "keys", key, query.ToArray());
                NameValueCollection data = BuildRequestData(null, value, ttl);
                string responseStr = null;
                WebExceptionStatus status = DoWebRequest(uri, "PUT", data, out responseStr);
                if (status == WebExceptionStatus.Success || status == WebExceptionStatus.ProtocolError)
                {
                    return EtcdResult.Parse(responseStr);
                }
            }
            return null;
        }

        public EtcdResult SetDir(string key, long? ttl = null)
        {

            foreach (EtcdServer srv in serverList)
            {
                string uri = BuildRequestUri(srv.host, srv.port, "keys", key);
                NameValueCollection data = BuildRequestData(true, null, ttl);
                string responseStr = null;
                WebExceptionStatus status = DoWebRequest(uri, "PUT", data, out responseStr);
                if (status == WebExceptionStatus.Success || status == WebExceptionStatus.ProtocolError)
                {
                    return EtcdResult.Parse(responseStr);
                }
            }
            return null;
        }

        public EtcdResult DeleteValue(string key, string prevValue = null, long? prevIndex = null)
        {
            List<string> query = new List<string>();
            if (prevValue != null)
            {
                query.Add("prevValue=" + WebUtility.UrlEncode(prevValue));
            }
            if (prevIndex != null)
            {
                query.Add("prevIndex=" + prevIndex.ToString());
            }
            foreach (EtcdServer srv in serverList)
            {
                string uri = BuildRequestUri(srv.host, srv.port, "keys", key, query.ToArray());
                string responseStr = null;
                WebExceptionStatus status = DoWebRequest(uri, "DELETE", out responseStr);
                if (status == WebExceptionStatus.Success || status == WebExceptionStatus.ProtocolError)
                {
                    return EtcdResult.Parse(responseStr);
                }
            }
            return null;
        }

        public EtcdResult DeleteDir(string key, bool recursive)
        {
            foreach (EtcdServer srv in serverList)
            {
                string uri = null;
                if (recursive)
                {
                    uri = BuildRequestUri(srv.host, srv.port, "keys", key, "recursive=true");
                }
                else
                {
                    uri = BuildRequestUri(srv.host, srv.port, "keys", key);
                }
                string responseStr = null;
                WebExceptionStatus status = DoWebRequest(uri, "DELETE", out responseStr);
                if (status == WebExceptionStatus.Success || status == WebExceptionStatus.ProtocolError)
                {
                    return EtcdResult.Parse(responseStr);
                }
            }
            return null;
        }

        public EtcdResult Enqueue(string key, string value, long? ttl = null)
        {
            foreach (EtcdServer srv in serverList)
            {
                string uri = BuildRequestUri(srv.host, srv.port, "keys", key);
                NameValueCollection data = BuildRequestData(null, value, ttl);
                string responseStr = null;
                WebExceptionStatus status = DoWebRequest(uri, "POST", data, out responseStr);
                if (status == WebExceptionStatus.Success || status == WebExceptionStatus.ProtocolError)
                {
                    return EtcdResult.Parse(responseStr);
                }
            }
            return null;
        }

        public EtcdResult Watch(string key, int waitIndex, out EtcdResult result)
        {
            throw new NotImplementedException();
        }

        public EtcdResult WatchAsync(string key, int waitIndex, AsyncCallback cb)
        {
            throw new NotImplementedException();
        }
    }
}
