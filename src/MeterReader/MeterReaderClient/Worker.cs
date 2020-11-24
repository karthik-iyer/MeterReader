using Google.Protobuf.WellKnownTypes;
using Grpc.Net.Client;
using MeterReaderWeb.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MeterReaderClient
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IConfiguration _config;
        private readonly ReadingFactory _factory;
        private MeterReadingService.MeterReadingServiceClient _client = null;

        public Worker(ILogger<Worker> logger, IConfiguration config , ReadingFactory factory)
        {
            _logger = logger;
            _config = config;
            _factory = factory;
        }

        protected MeterReadingService.MeterReadingServiceClient Client
        {
            get
            {
                 if(_client == null)
                {
                    var channel = GrpcChannel.ForAddress(_config["Service:ServerUrl"]);
                    _client = new MeterReadingService.MeterReadingServiceClient(channel);
                }
                return _client;
            }
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);

                var customerId = _config.GetValue<int>("Service:CustomerId");               

                var pkt = new ReadingPacket
                {
                    Successful = ReadingStatus.Success,
                    Notes = "This is Test"           
                };

                for(var x = 0; x < 10; x++)
                {
                    pkt.Readings.Add(await _factory.Generate(customerId));
                }

                var result = await Client.AddReadingAsync(pkt);

                if(result.Success == ReadingStatus.Success)
                {
                    _logger.LogInformation("Successfully sent");
                }
                else
                {
                    _logger.LogInformation("Failed to send");
                }

                await Task.Delay(_config.GetValue<int>("Service:DelayInterval"), stoppingToken);
            }
        }
    }
}
