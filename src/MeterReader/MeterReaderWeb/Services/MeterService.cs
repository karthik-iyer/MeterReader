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
                catch(Exception ex)
                {
                    result.Message = "Exception thrown when trying to save to Database";
                    _logger.LogError($"Execption thrown during saving of the readings: {ex}");
                }
            }

            return result;
        }
    }
}
