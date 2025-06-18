import { Inject, Injectable, Logger } from "@nestjs/common";
import { SqsService } from "../services/sqs.service";
import { SQSMessage, ReceivedMessage } from "../../../common/types/sqs";
import {
  ReceiveMessageCommand,
  DeleteMessageCommand,
  ReceiveMessageCommandInput,
} from "@aws-sdk/client-sqs";
import { EthersService } from "../../ethers/ethers.service";

@Injectable()
export class SqsConsumer {
  constructor(
    private readonly sqsService: SqsService,
    @Inject(Logger)
    private readonly logger = new Logger(SqsConsumer.name),
    private readonly ethersService: EthersService
  ) {}

  async receive(): Promise<ReceivedMessage[]> {
    const sqs = this.sqsService.getSQSCliendt();
    const queueUrl = this.sqsService.getQueueUrl();
    const params: ReceiveMessageCommandInput = {
      QueueUrl: queueUrl,
      AttributeNames: ["All"],
      MaxNumberOfMessages: 10,
      MessageAttributeNames: ["All"],
      VisibilityTimeout: 90,
      WaitTimeSeconds: 20,
    };
    const command = new ReceiveMessageCommand(params);

    try {
      const data = await sqs.send(command);

      if (data.Messages && data.Messages.length > 0) {
        return data.Messages.map((msg: SQSMessage) => ({
          ...msg,
          Body: msg.Body,
          ReceiptHandle: msg.ReceiptHandle,
        }));
      } else {
        console.log("No messages to receive");
        return [];
      }
    } catch (err) {
      this.logger.error(
        `[sqs - receive] : {
          "error":"${err.message}"
        }`
      );
      return [];
    }
  }

  async excuteProcess() {
    const executeSyncElements = {
      messageId: "",
      messageHandle: "",
    };
    const messages = await this.receive();
    let nonce = await this.ethersService.getNonce(
      this.ethersService.getAccount1()
    );

    for (const message of messages) {
      try {
        executeSyncElements.messageId = message.MessageId;
        executeSyncElements.messageHandle = message.ReceiptHandle;
        const body = message.Body;

        if (body) {
          // console.log('무엇이 나오나?', body);

          await this.ethersService.send1ETH(nonce++);
          await this.delete(
            executeSyncElements.messageHandle,
            executeSyncElements.messageId
          );
        } else {
          await this.delete(
            executeSyncElements.messageHandle,
            executeSyncElements.messageId
          );
          
          // DB 저장하기
          continue;
        }
      } catch (err) {
        this.logger.error(
          `[sqs - excuteProcess] : {
            "error":"${err.message}"
          }`
        );

        await this.delete(
          executeSyncElements.messageHandle,
          executeSyncElements.messageId
        );

        // DB 저장하기
        continue;
      }
    }
  }

  async delete(messageReceiptHandle: string, messageId: string) {
    const queueUrl = this.sqsService.getQueueUrl();
    const sqs = this.sqsService.getSQSCliendt();
    const params = {
      QueueUrl: queueUrl,
      ReceiptHandle: messageReceiptHandle,
    };
    const command = new DeleteMessageCommand(params);

    try {
      await sqs.send(command);
    } catch (err) {
      if (err.code === "InvalidParameterValueException") {
        this.logger.error(
          `[sqs - delete] : {
            "input": {
              "messageReceiptHandle":"${JSON.stringify(messageReceiptHandle)}",
              "messageId":"${JSON.stringify(messageId)}"
            },
            "error":"${err.message}"
          }`
        );
      } else {
        this.logger.error(
          `[sqs - delete] : {
            "input": {
              "messageReceiptHandle":"${JSON.stringify(messageReceiptHandle)}",
              "messageId":"${JSON.stringify(messageId)}"
            },
            "error":"${err.message}"
          }`
        );
      }
    }
  }
}
