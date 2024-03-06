using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Microsoft.AspNetCore.Mvc;

namespace WeatherAPI.Controllers
{
    [ApiController]
    //[Route("weather")]
    public class WeatherForecastController : ControllerBase
    {
        static readonly RegionEndpoint region = RegionEndpoint.EUNorth1;

        private readonly ILogger<WeatherForecastController> _logger;

        public WeatherForecastController(ILogger<WeatherForecastController> logger)
        {
            _logger = logger;
        }

        [HttpGet("/")]
        public string GetHealthcheck()
        {
            return "Healthcheck: Healthy";
        }

        [HttpGet("/weatherforecast")]
        public async  Task<IEnumerable<WeatherForecast>> GetWeatherForecast(string location = "Dallas")
        {
            List<WeatherForecast> forecast = new();

            try
            {
                _logger.LogInformation($"00 enter GET, location = {location}");

                var client = new AmazonDynamoDBClient(region);
                Table table = Table.LoadTable(client, "Weather");

                var filter = new ScanFilter();
                filter.AddCondition("Location", ScanOperator.Equal, location);

                var scanConfig = new ScanOperationConfig()
                {
                    Filter = filter,
                    Select = SelectValues.SpecificAttributes,
                    AttributesToGet = new List<string> { "Location", "Timestamp", "TempC", "TempF", "Summary" }
                };

                _logger.LogInformation($"10 table.Scan");

                Search search = table.Scan(scanConfig);

                List<Document> matches;
                do
                {
                    _logger.LogInformation($"20 table.GetNextSetAsync");
                    matches = await search.GetNextSetAsync();
                    foreach (var match in matches)
                    {
                        forecast.Add(new WeatherForecast
                        {
                            Location = Convert.ToString(match["Location"]),
                            Date = Convert.ToDateTime(match["Timestamp"]),
                            TemperatureC = Convert.ToInt32(match["TempC"]),
                            TemperatureF = Convert.ToInt32(match["TempF"]),
                            Summary = Convert.ToString(match["Summary"])
                        });
                    }
                } while (!search.IsDone);

                _logger.LogInformation($"30 exited results loop");
            }
            catch(Exception ex)
            {
                _logger.LogError(ex, "90 Exception");
            }

            _logger.LogInformation($"99 returning {forecast.Count} results");

            return forecast.ToArray();
        }
    }
}
