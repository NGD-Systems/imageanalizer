// Copyright (c) Microsoft. All rights reserved.
namespace imageAnalizerII
{
    using System;
    using System.IO;
    using System.Net;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Devices.Client;
    using Microsoft.Azure.Devices.Client.Transport.Mqtt;
    using Microsoft.Azure.Devices.Edge.Util;
    using Microsoft.Azure.Devices.Edge.Util.Concurrency;
    using Microsoft.Azure.Devices.Edge.Util.TransientFaultHandling;
    using Microsoft.Azure.Devices.Shared;
    using Microsoft.Extensions.Configuration;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using System.Net.Http;
    using System.Net.Http.Headers;


    using ExponentialBackoff = Microsoft.Azure.Devices.Edge.Util.TransientFaultHandling.ExponentialBackoff;

    class Program
    {
        const string MessageCountConfigKey = "MessageCount";
        const string SendDataConfigKey = "SendData";
        const string SendIntervalConfigKey = "SendInterval";

        static readonly ITransientErrorDetectionStrategy DefaultTimeoutErrorDetectionStrategy =
            new DelegateErrorDetectionStrategy(ex => ex.HasTimeoutException());

        static readonly RetryStrategy DefaultTransientRetryStrategy =
            new ExponentialBackoff(
                5,
                TimeSpan.FromSeconds(2),
                TimeSpan.FromSeconds(60),
                TimeSpan.FromSeconds(4));

        static readonly Guid BatchId = Guid.NewGuid();
        static readonly AtomicBoolean Reset = new AtomicBoolean(false);
        static readonly Random Rnd = new Random();
        static TimeSpan messageDelay;
        static bool sendData = true;

        static string subscriptionKey = Environment.GetEnvironmentVariable("COMPUTER_VISION_SUBSCRIPTION_KEY");

        static string endpoint = Environment.GetEnvironmentVariable("COMPUTER_VISION_ENDPOINT");

        // the Analyze method endpoint
        static string uriBase = endpoint + "vision/v2.1/analyze";


        public enum ControlCommandEnum
        {
            Reset = 0,
            NoOperation = 1
        }

        public static bool keepon = true;

        public static int Main() => MainAsync().Result;

        static async Task<int> MainAsync()
        {
            Console.WriteLine("Image Analizer Main() started.");

            IConfiguration configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("config/appsettings.json", optional: true)
                .AddEnvironmentVariables()
                .Build();

            //string[] arguments = Environment.GetCommandLineArgs();
            //string imageFilePath = arguments[1];
            string analisys;
            while (keepon)
            {
                if (Directory.Exists("/data/input"))
                {
                    foreach (string imageFilePath in Directory.EnumerateFiles("/data/input"))
                    {
                        Console.WriteLine($"Analyzing image file: {imageFilePath} ......");
                        if (File.Exists(imageFilePath))
                        {
                            // Call the REST API method.
                            Console.WriteLine(imageFilePath);
                            Console.WriteLine(subscriptionKey);
                            Console.WriteLine(endpoint);
                            Console.WriteLine(uriBase);
                            Console.WriteLine("\nWait for the results to appear.\n");
                            analisys = await MakeAnalysisRequest(imageFilePath);
                        }
                        else
                        {
                            Console.WriteLine("\nInvalid file path");
                            return 1;
                        }

                        messageDelay = configuration.GetValue("MessageDelay", TimeSpan.FromSeconds(5));
                        int messageCount = configuration.GetValue(MessageCountConfigKey, 500);
                        var simulatorParameters = new SimulatorParameters
                        {
                            MachineTempMin = configuration.GetValue<double>("machineTempMin", 21),
                            MachineTempMax = configuration.GetValue<double>("machineTempMax", 100),
                            MachinePressureMin = configuration.GetValue<double>("machinePressureMin", 1),
                            MachinePressureMax = configuration.GetValue<double>("machinePressureMax", 10),
                            AmbientTemp = configuration.GetValue<double>("ambientTemp", 21),
                            HumidityPercent = configuration.GetValue("ambientHumidity", 25)
                        };

                        //Console.WriteLine(
                        //    $"Initializing simulated temperature sensor to send {(SendUnlimitedMessages(messageCount) ? "unlimited" : messageCount.ToString())} "
                        //    + $"messages, at an interval of {messageDelay.TotalSeconds} seconds.\n"
                        //    + $"To change this, set the environment variable {MessageCountConfigKey} to the number of messages that should be sent (set it to -1 to send unlimited messages).");

                        TransportType transportType = configuration.GetValue("ClientTransportType", TransportType.Amqp_Tcp_Only);

                        ModuleClient moduleClient = await CreateModuleClientAsync(
                            transportType,
                            DefaultTimeoutErrorDetectionStrategy,
                            DefaultTransientRetryStrategy);
                        Console.WriteLine("passed CreateModuleClientAsync");
                        await moduleClient.OpenAsync();
                        Console.WriteLine("passed OpenAsync");
                        await moduleClient.SetMethodHandlerAsync("reset", ResetMethod, null);
                        Console.WriteLine("passed SetMethodHandlerAsync");

                        (CancellationTokenSource cts, ManualResetEventSlim completed, Option<object> handler) = ShutdownHandler.Init(TimeSpan.FromSeconds(5), null);

                        //Twin currentTwinProperties = await moduleClient.GetTwinAsync();
                        //Console.WriteLine("passed GetTwinAsync");
                        //if (currentTwinProperties.Properties.Desired.Contains(SendIntervalConfigKey))
                        //{
                        //    messageDelay = TimeSpan.FromSeconds((int)currentTwinProperties.Properties.Desired[SendIntervalConfigKey]);
                        //}

                        //if (currentTwinProperties.Properties.Desired.Contains(SendDataConfigKey))
                        //{
                        //    sendData = (bool)currentTwinProperties.Properties.Desired[SendDataConfigKey];
                        //    if (!sendData)
                        //    {
                        //        Console.WriteLine("Sending data disabled. Change twin configuration to start sending again.");
                        //    }
                        // }

                        ModuleClient userContext = moduleClient;
                        //await moduleClient.SetDesiredPropertyUpdateCallbackAsync(OnDesiredPropertiesUpdated, userContext);
                        Console.WriteLine("before SetInputMessageHandlerAsync");
                        await moduleClient.SetInputMessageHandlerAsync("control", ControlMessageHandle, userContext);
                        Console.WriteLine("passed moduleClient.SetInputMessageHandlerAsync");
                        await SendEvents(moduleClient, analisys, cts);
                        Console.WriteLine("Message sent......");
                        //await cts.Token.WhenCanceled();
                    }
                    Console.WriteLine("Done processing images. Sleeping for 30 seconds...");
                    await Task.Delay(30000);
                }
                else
                {
                    Console.WriteLine("missing data directory. exiting ...");
                    return 1;
                }
            }
            //completed.Set();
            //handler.ForEach(h => GC.KeepAlive(h));
            return 0;
        }

        static async Task<string> MakeAnalysisRequest(string imageFilePath)
        {
            try
            {
                HttpClient client = new HttpClient();

                // Request headers.
                client.DefaultRequestHeaders.Add(
                    "Ocp-Apim-Subscription-Key", subscriptionKey);

                // Request parameters. A third optional parameter is "details".
                // The Analyze Image method returns information about the following
                // visual features:
                // Categories:  categorizes image content according to a
                //              taxonomy defined in documentation.
                // Description: describes the image content with a complete
                //              sentence in supported languages.
                // Color:       determines the accent color, dominant color, 
                //              and whether an image is black & white.
                string requestParameters =
                    "visualFeatures=Categories,Description,Color";

                // Assemble the URI for the REST API method.
                string uri = uriBase + "?" + requestParameters;

                HttpResponseMessage response;

                // Read the contents of the specified local image
                // into a byte array.
                byte[] byteData = GetImageAsByteArray(imageFilePath);

                // Add the byte array as an octet stream to the request body.
                using (ByteArrayContent content = new ByteArrayContent(byteData))
                {
                    // This example uses the "application/octet-stream" content type.
                    // The other content types you can use are "application/json"
                    // and "multipart/form-data".
                    content.Headers.ContentType =
                        new MediaTypeHeaderValue("application/octet-stream");

                    // Asynchronously call the REST API method.
                    response = await client.PostAsync(uri, content);
                }

                // Asynchronously get the JSON response.
                string contentString = await response.Content.ReadAsStringAsync();

                // Display the JSON response.
                Console.WriteLine("\nResponse:\n\n{0}\n",
                    JToken.Parse(contentString).ToString());

                return contentString;

            }
            catch (Exception e)
            {
                Console.WriteLine("\n" + e.Message);
                return e.Message;
            }

        }

        /// <summary>
        /// Returns the contents of the specified file as a byte array.
        /// </summary>
        /// <param name="imageFilePath">The image file to read.</param>
        /// <returns>The byte array of the image data.</returns>
        static byte[] GetImageAsByteArray(string imageFilePath)
        {
            // Open a read-only file stream for the specified file.
            using (FileStream fileStream =
                new FileStream(imageFilePath, FileMode.Open, FileAccess.Read))
            {
                // Read the file's contents into a byte array.
                BinaryReader binaryReader = new BinaryReader(fileStream);
                return binaryReader.ReadBytes((int)fileStream.Length);
            }
        }

        static bool SendUnlimitedMessages(int maximumNumberOfMessages) => maximumNumberOfMessages < 0;

        // Control Message expected to be:
        // {
        //     "command" : "reset"
        // }
        static Task<MessageResponse> ControlMessageHandle(Message message, object userContext)
        {
            byte[] messageBytes = message.GetBytes();
            string messageString = Encoding.UTF8.GetString(messageBytes);

            Console.WriteLine($"Received message Body: [{messageString}]");

            try
            {
                var messages = JsonConvert.DeserializeObject<ControlCommand[]>(messageString);

                foreach (ControlCommand messageBody in messages)
                {
                    if (messageBody.Command == ControlCommandEnum.Reset)
                    {
                        Console.WriteLine("Resetting temperature sensor..");
                        Reset.Set(true);
                    }
                }
            }
            catch (JsonSerializationException)
            {
                var messageBody = JsonConvert.DeserializeObject<ControlCommand>(messageString);

                if (messageBody.Command == ControlCommandEnum.Reset)
                {
                    Console.WriteLine("Resetting temperature sensor..");
                    Reset.Set(true);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: Failed to deserialize control command with exception: [{ex}]");
            }

            return Task.FromResult(MessageResponse.Completed);
        }

        static Task<MethodResponse> ResetMethod(MethodRequest methodRequest, object userContext)
        {
            Console.WriteLine("Received direct method call to reset temperature sensor...");
            Reset.Set(true);
            var response = new MethodResponse((int)HttpStatusCode.OK);
            return Task.FromResult(response);
        }

        /// <summary>
        /// Module behavior:
        ///        Sends data periodically (with default frequency of 5 seconds).
        ///        Data trend:
        ///         - Machine Temperature regularly rises from 21C to 100C in regularly with jitter
        ///         - Machine Pressure correlates with Temperature 1 to 10psi
        ///         - Ambient temperature stable around 21C
        ///         - Humidity is stable with tiny jitter around 25%
        ///                Method for resetting the data stream.
        /// </summary>
        static async Task SendEvents(
            ModuleClient moduleClient,
            string message,
            CancellationTokenSource cts)
        {
            //int count = 1;
            //double currentTemp = sim.MachineTempMin;
            //double normal = (sim.MachinePressureMax - sim.MachinePressureMin) / (sim.MachineTempMax - sim.MachineTempMin);

            //while (!cts.Token.IsCancellationRequested && (SendUnlimitedMessages(messageCount) || messageCount >= count))
            //{
            //    if (Reset)
            //    {
            //        currentTemp = sim.MachineTempMin;
            //        Reset.Set(false);
            //    }
            //
            //    if (currentTemp > sim.MachineTempMax)
            //    {
            //        currentTemp += Rnd.NextDouble() - 0.5; // add value between [-0.5..0.5]
            //    }
            //    else
            //    {
            //        currentTemp += -0.25 + (Rnd.NextDouble() * 1.5); // add value between [-0.25..1.25] - average +0.5
            //    }
            //
            //if (sendData)
            //{
            //    var tempData = new MessageBody
            //    {
            //        Machine = new Machine
            //        {
            //            Temperature = currentTemp,
            //            Pressure = sim.MachinePressureMin + ((currentTemp - sim.MachineTempMin) * normal),
            //        },
            //        Ambient = new Ambient
            //        {
            //            Temperature = sim.AmbientTemp + Rnd.NextDouble() - 0.5,
            //            Humidity = Rnd.Next(24, 27)
            //        },
            //        TimeCreated = DateTime.UtcNow
            //    };

            //string dataBuffer = JsonConvert.SerializeObject(tempData);
            string dataBuffer = JsonConvert.SerializeObject(message);
            var eventMessage = new Message(Encoding.UTF8.GetBytes(dataBuffer));
            eventMessage.Properties.Add("sequenceNumber", "0");
            eventMessage.Properties.Add("batchId", BatchId.ToString());
            Console.WriteLine($"\t{DateTime.Now.ToLocalTime()}> Sending message: 0, Body: [{dataBuffer}]");

            await moduleClient.SendEventAsync("imageanalisys", eventMessage);
            //    count++;
            //}

            //await Task.Delay(messageDelay, cts.Token);
            //}

            //if (messageCount < count)
            //{
            //    Console.WriteLine($"Done sending {messageCount} messages");
            //}
        }

        static async Task OnDesiredPropertiesUpdated(TwinCollection desiredPropertiesPatch, object userContext)
        {
            // At this point just update the configure configuration.
            if (desiredPropertiesPatch.Contains(SendIntervalConfigKey))
            {
                messageDelay = TimeSpan.FromSeconds((int)desiredPropertiesPatch[SendIntervalConfigKey]);
            }

            if (desiredPropertiesPatch.Contains(SendDataConfigKey))
            {
                bool desiredSendDataValue = (bool)desiredPropertiesPatch[SendDataConfigKey];
                if (desiredSendDataValue != sendData && !desiredSendDataValue)
                {
                    Console.WriteLine("Sending data disabled. Change twin configuration to start sending again.");
                }

                sendData = desiredSendDataValue;
            }

            var moduleClient = (ModuleClient)userContext;
            var patch = new TwinCollection($"{{ \"SendData\":{sendData.ToString().ToLower()}, \"SendInterval\": {messageDelay.TotalSeconds}}}");
            await moduleClient.UpdateReportedPropertiesAsync(patch); // Just report back last desired property.
        }

        static async Task<ModuleClient> CreateModuleClientAsync(
            TransportType transportType,
            ITransientErrorDetectionStrategy transientErrorDetectionStrategy = null,
            RetryStrategy retryStrategy = null)
        {
            var retryPolicy = new RetryPolicy(transientErrorDetectionStrategy, retryStrategy);
            retryPolicy.Retrying += (_, args) => { Console.WriteLine($"[Error] Retry {args.CurrentRetryCount} times to create module client and failed with exception:{Environment.NewLine}{args.LastException}"); };

            ModuleClient client = await retryPolicy.ExecuteAsync(
                async () =>
                {
                    ITransportSettings[] GetTransportSettings()
                    {
                        switch (transportType)
                        {
                            case TransportType.Mqtt:
                            case TransportType.Mqtt_Tcp_Only:
                                return new ITransportSettings[] { new MqttTransportSettings(TransportType.Mqtt_Tcp_Only) };
                            case TransportType.Mqtt_WebSocket_Only:
                                return new ITransportSettings[] { new MqttTransportSettings(TransportType.Mqtt_WebSocket_Only) };
                            case TransportType.Amqp_WebSocket_Only:
                                return new ITransportSettings[] { new AmqpTransportSettings(TransportType.Amqp_WebSocket_Only) };
                            default:
                                return new ITransportSettings[] { new AmqpTransportSettings(TransportType.Amqp_Tcp_Only) };
                        }
                    }

                    ITransportSettings[] settings = GetTransportSettings();
                    Console.WriteLine($"[Information]: Trying to initialize module client using transport type [{transportType}].");
                    ModuleClient moduleClient = await ModuleClient.CreateFromEnvironmentAsync(settings);
                    await moduleClient.OpenAsync();

                    Console.WriteLine($"[Information]: Successfully initialized module client of transport type [{transportType}].");
                    return moduleClient;
                });

            return client;
        }

        class ControlCommand
        {
            [JsonProperty("command")]
            public ControlCommandEnum Command { get; set; }
        }

        class SimulatorParameters
        {
            public double MachineTempMin { get; set; }

            public double MachineTempMax { get; set; }

            public double MachinePressureMin { get; set; }

            public double MachinePressureMax { get; set; }

            public double AmbientTemp { get; set; }

            public int HumidityPercent { get; set; }
        }
    }
}
