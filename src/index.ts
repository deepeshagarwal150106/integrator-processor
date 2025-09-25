import { PrismaClient } from "@prisma/client";
import { Kafka } from "kafkajs";

const TOPIC_NAME = "zap-events";

const client = new PrismaClient();

const BROKER = process.env.BROKER || "localhost:9092";
const kafka = new Kafka({
  clientId: "outboc-processor",
  brokers: [BROKER],
  ssl: {
    rejectUnauthorized: false
  }
});

async function main() {
  try {
    const producer = kafka.producer();
    await producer.connect();
    while (true) { 
      const pendingRows = await client.zapRunOutBox.findMany({
        where: {},
        take: 10
      });
      producer.send({
        topic: TOPIC_NAME,
        messages: pendingRows.map(row => {
          return {
            value: JSON.stringify({ zapRunId: row.zapRunId, stage: 0 }),
            key: row.id.toString()
          };
        })
      });
      await client.zapRunOutBox.deleteMany({
        where: {
            id: {
                in: pendingRows.map(row => row.id)
            }
        }
      });
    }
  } catch (error) {
    console.error("Error in processor:", error);
  }

}
main().catch(console.error);
