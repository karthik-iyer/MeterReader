using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using MeterReaderWeb.Data;
using MeterReaderWeb.Data.Entities;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace MeterReaderWeb.Services
{
    public class MeterService : MeterReadingService.MeterReadingServiceBase
    {
        private readonly ILogger<MeterService> _logger;
        private readonly IReadingRepository _repository;

        public MeterService(ILogger<MeterService> logger, IReadingRepository readingRepository)
        {
            _logger = logger;
            _repository = readingRepository;

        }


        public override async Task<Empty> SendDiagnostics(IAsyncStreamReader<ReadingMessage> requestStream, ServerCallContext context)
        {
            var theTask = Task.Run(async () =>
            { 
                await foreach(var reading in requestStream.ReadAllAsync())
                {
                    _logger.LogInformation($"Received Reading : {reading}");
                }
            });

            await theTask;

            
            return new Empty();
        }

        public override async Task<StatusMessage> AddReading(ReadingPacket request, ServerCallContext context)
        {
            var result = new StatusMessage
            {
                Success = ReadingStatus.Failure
            };

            if(request.Successful == ReadingStatus.Success)
            {
                try
                {
                    foreach(var r in request.Readings)
                    {

                        if(r.ReadingValue < 1000)
                        {
                            _logger.LogDebug("Reading Value below acceptable level");
                            var trailer = new Metadata()
                            {
                                { "BadData" ,r.ReadingValue.ToString()},
                                { "Field" ,"Reading Value"},
                                { "Message" ,"Readings are invalid"}
                            };

                            throw new RpcException(new Status(StatusCode.OutOfRange, "Value Too low"),trailer);
                        }

                        //Save it to DB
                        var reading = new MeterReading(){
                           CustomerId = r.CustomerId,
                           ReadingDate = r.ReadingTime.ToDateTime(),
                           Value = r.ReadingValue
                        };
                        _repository.AddEntity(reading);
                        
                    }

                    if(await _repository.SaveAllAsync())
                    {
                        _logger.LogInformation($"Stored {request.Readings.Count} New Readings...");
                        result.Success = ReadingStatus.Success;
                    }

                }
                catch(RpcException)
                {
                    throw;
                }
                catch(Exception ex)
                {
                    _logger.LogError($"Execption thrown during saving of the readings: {ex}");
                    throw new RpcException(Status.DefaultCancelled, "Exception thrown when trying to save to Database");
                   
                }
            }

            return result;
        }
    }
}
