using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using SQLite;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using System.Xml;

namespace MediaWikiCrawler
{
    class Program
    {
        const string PATH_TO_API_PHP = "http://evewiki.tiancity.com/api.php";
        const string WIKI_NAME = "evewiki";

        static void Main(string[] args)
        {
            var program = new Program();
            program.FetchPages().GetAwaiter().GetResult();
        }

        async Task FetchPages()
        {
            var outputCount = 0;
            var queryCount = 0;
            
            var client = new HttpClient();
            var uriBuilder = new UriBuilder(PATH_TO_API_PHP);
            
            File.Delete(WIKI_NAME);
            var db = new SQLiteConnection(WIKI_NAME);
            db.CreateTable<WikiPage>();

            // get params for exporting pages, 500 at a time
            // 用于导出 wiki 页面的 GET 参数，一次只能获取 500 页
            var continueFrom = "";
            var uriQuery = new Dictionary<string, string>()
            {
                {"action", "query" },
                {"generator", "allpages" },
                {"gaplimit", "500" },
                {"gapcontinue", continueFrom },
                {"format", "json" },
                {"export", "" }
            };

            do
            {
                // update gapcontinue param
                // 更新 gapcontinue 参数
                Console.WriteLine($"Query No. {++queryCount}, continuing from {continueFrom}");
                uriQuery["gapcontinue"] = continueFrom;
                uriBuilder.Query = new FormUrlEncodedContent(uriQuery).ReadAsStringAsync().Result;
                Console.WriteLine($"Query string: {uriBuilder.ToString()}");

                // read from server
                // 服务器请求
                var fullJson = await client.GetAsync(uriBuilder.ToString());
                var fullJobject = JObject.Parse(await fullJson.Content.ReadAsStringAsync());
                continueFrom = (string)fullJobject.SelectToken("continue.gapcontinue");

                // get where to continue from
                // 获取 continue 参数以供下次请求
                var contentXml = (string)fullJobject.SelectToken("query.export.*");
                var contentXmlDoc = new XmlDocument();
                contentXmlDoc.LoadXml(contentXml);
                var contentJson = JsonConvert.SerializeXmlNode(contentXmlDoc);
                if (String.IsNullOrEmpty(continueFrom)) continueFrom = "";

                // write json; replace slashes to prevent path issues
                // 写入 json 结果，把斜杠都替换掉，防止抛出路径异常
                File.WriteAllText($"export-continuefrom-" +
                    $"{continueFrom.Replace('/', '%').Replace('\\', '%')}" +
                    $".json", contentJson);

                var pages = JObject.Parse(contentJson);
                Console.WriteLine($"In this batch, first entry is " +
                    $"{pages.SelectToken("mediawiki.page").ToArray().First()}");
                Console.WriteLine($"In this batch, last entry is " +
                    $"{pages.SelectToken("mediawiki.page").ToArray().Last()}");

                db.BeginTransaction();
                foreach (var page in pages.SelectToken("mediawiki.page").ToArray())
                {
                    Int32.TryParse((string)page.SelectToken("id"), out int pageId);
                    Int32.TryParse((string)page.SelectToken("revision.contributor.id"), out int contId);
                    Int32.TryParse((string)page.SelectToken("revision.text.@bytes"), out int textBytes);
                    try
                    {
                        db.Insert(new WikiPage()
                        {
                            id = pageId,
                            title = (string)page.SelectToken("title"),
                            contributor_timestamp = (string)page.SelectToken("revision.timestamp"),
                            contributor_id = contId,
                            contributor_name = (string)page.SelectToken("revision.contributor.username"),
                            text_bytes = textBytes,
                            text = (string)page.SelectToken("revision.text.#text")
                        });
                        outputCount++;
                    }
                    catch { }
                }
                db.Commit();
                Console.WriteLine("Written " + outputCount + " entries...\n");
            }
            while (!String.IsNullOrEmpty(continueFrom));

            db.Close();
            Console.WriteLine("Done writing " + outputCount + " entries.");
            Console.ReadKey();
        }
    }

    public class WikiPage
    {
        [PrimaryKey]
        public int id { get; set; }

        public string title { get; set; }

        public string contributor_timestamp { get; set; }

        public int contributor_id { get; set; }

        public string contributor_name { get; set; }

        public int text_bytes { get; set; }

        public string text { get; set; }
    }
}
