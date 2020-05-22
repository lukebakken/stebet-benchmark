using System;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace stebet_benchmar
{
    class Program
    {
        private static int messagesSent = 0;
        private static int messagesReceived = 0;
        private static int batchesToSend = 100;
        private static int itemsPerBatch = 500;
        static async Task Main(string[] args)
        {
            ThreadPool.SetMinThreads(16 * Environment.ProcessorCount, 16 * Environment.ProcessorCount);
            var connectionString = new Uri("amqp://guest:guest@localhost/");

            var connectionFactory = new ConnectionFactory() { DispatchConsumersAsync = true, Uri = connectionString };
            var connection = connectionFactory.CreateConnection();
            var publisher = connection.CreateModel();
            var subscriber = connection.CreateModel();

            publisher.ConfirmSelect();
            publisher.ExchangeDeclare("test", ExchangeType.Topic, false, false);

            subscriber.QueueDeclare("testqueue", false, false, true);
            var asyncListener = new AsyncEventingBasicConsumer(subscriber);
            asyncListener.Received += AsyncListener_Received;
            subscriber.QueueBind("testqueue", "test", "myawesome.routing.key");
            subscriber.BasicConsume("testqueue", true, "testconsumer", asyncListener);

            byte[] payload = new byte[512];
            var batchPublish = Task.Run(() =>
            {
                while (messagesSent < batchesToSend * itemsPerBatch)
                {
                    var batch = publisher.CreateBasicPublishBatch();
                    for (int i = 0; i < itemsPerBatch; i++)
                    {
                        var properties = publisher.CreateBasicProperties();
                        properties.AppId = "testapp";
                        properties.CorrelationId = Guid.NewGuid().ToString();
                        batch.Add("test", "myawesome.routing.key", false, properties, payload);
                    }
                    batch.Publish();
                    messagesSent += itemsPerBatch;
                    publisher.WaitForConfirmsOrDie();
                }
            });

            var sentTask = Task.Run(async () =>
            {
                while (messagesSent < batchesToSend * itemsPerBatch)
                {
                    Console.WriteLine($"Messages sent: {messagesSent}");

                    await Task.Delay(500);
                }

                Console.WriteLine("Done sending messages!");
            });

            var receivedTask = Task.Run(async () =>
            {
                while (messagesReceived < batchesToSend * itemsPerBatch)
                {
                    Console.WriteLine($"Messages received: {messagesReceived}");

                    await Task.Delay(500);
                }

                Console.WriteLine("Done receiving all messages.");
            });

            await Task.WhenAll(sentTask, receivedTask);

            publisher.Dispose();
            subscriber.Dispose();
            connection.Dispose();
            Console.ReadLine();
        }

        private static Task AsyncListener_Received(object sender, BasicDeliverEventArgs @event)
        {
            Interlocked.Increment(ref messagesReceived);
            return Task.CompletedTask;
        }
    }
}
