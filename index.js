const { Kafka } = require('kafkajs');

const broker = process.env.KAFKA_BROKER || 'kafka:9093';
console.log(`Attempting to connect to Kafka broker at ${broker}`);

const kafka = new Kafka({
  clientId: 'my-nodejs-app',
  brokers: [broker]
});

async function run() {
  const producer = kafka.producer();
  const consumer = kafka.consumer({ groupId: 'test-group' });

  let connected = false;
  while (!connected) {
    try {
      await producer.connect();
      console.log('Producer connected successfully');
      await consumer.connect();
      console.log('Consumer connected successfully');
      connected = true;
    } catch (error) {
      console.error('Connection failed, retrying in 5 seconds...', error.message);
      await new Promise((resolve) => setTimeout(resolve, 5000));
    }
  }

  // Producer send message
  await producer.send({
    topic: 'my-topic',
    messages: [{ value: 'Hello KafkaJS user!' }],
  });
  console.log('Message sent');

  // Consumer subscribe
  await consumer.subscribe({ topic: 'my-topic', fromBeginning: true });
  console.log(`Subscribed to topic 'my-topic'`);

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        partition,
        offset: message.offset,
        value: message.value.toString(),
      });
    },
  });
}

run().catch(console.error);