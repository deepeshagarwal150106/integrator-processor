import { PrismaClient } from "@prisma/client";
import { Kafka } from "kafkajs";
import fs from "fs";
import path from "path";

const TOPIC_NAME = "zap-events";

const client = new PrismaClient();

const BROKER = process.env.BROKER || "localhost:9092";
const kafka = new Kafka({
  clientId: "outboc-processor",
  brokers: [BROKER],
  ssl: {
    rejectUnauthorized: true, // Recommended for production
    // Read the certificate files from your 'certs' folder
    ca: [fs.readFileSync(path.join(__dirname, 'certs/ca.pem'), 'utf-8')],
    key: fs.readFileSync(path.join(__dirname, 'certs/service.key'), 'utf-8'),
    cert: fs.readFileSync(path.join(__dirname, 'certs/service.cert'), 'utf-8')
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
