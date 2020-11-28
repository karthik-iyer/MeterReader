using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using MeterReaderLib;
using MeterReaderLib.Models;
using MeterReaderWeb.Data;
using MeterReaderWeb.Data.Entities;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

namespace MeterReaderWeb.Services
{
    [Authorize(AuthenticationSchemes = JwtBearerDefaults.AuthenticationScheme)]
    public class MeterService : MeterReadingService.MeterReadingServiceBase
    {
        private readonly ILogger<MeterService> _logger;
        private readonly IReadingRepository _repository;
        private readonly JwtTokenValidationService _tokenService;

        public MeterService(ILogger<MeterService> logger, IReadingRepository readingRepository, JwtTokenValidationService tokenService)
        {
            _logger = logger;
            _repository = readingRepository;
            _tokenService = tokenService;
        }

        [AllowAnonymous]
        public override async Task<TokenResponse> CreateToken(TokenRequest request, ServerCallContext context)
        {
            var creds = new CredentialModel()
            {
                UserName = request.Username,
                Passcode = request.Password
            };

            var response = await _tokenService.GenerateTokenModelAsync(creds);

            if (response.Success)
            {
                return new TokenResponse()
                {
                    Token = response.Token,
                    Expiration = Timestamp.FromDateTime(response.Expiration),
                    Success = response.Success
                };
            }

            return new TokenResponse()
            {
                Success = response.Success
            };
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
