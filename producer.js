const { Kafka } = require('kafkajs');
const readline = require("readline");

const kafka = new Kafka({
    clientId:'my-app',
    brokers:['192.168.1.67:9092'],
});



const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  });   

async function init(){
    const producer = kafka.producer();

    console.log("Producer connecting...");
    await producer.connect();
    console.log("Producer connecting successfully...");

    rl.setPrompt("> ");
    rl.prompt();

    rl.on("line", async function (line) {
        const [riderName, location] = line.split(" ");
        await producer.send({
          topic: "rider-updates",
          messages: [
            {
              partition: location.toLowerCase() === "north" ? 0 : 1,
              key: "location-update",
              value: JSON.stringify({ name: riderName, location }),
            },
          ],
        });
      }).on("close", async () => {
        await producer.disconnect();
      });
}

init();