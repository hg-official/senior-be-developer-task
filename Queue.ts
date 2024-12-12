import { Message } from "./Database";

/**
 * We need to guarantee consistency for a certain Item. Parallel processing of different messages for a specific Item by different Workers can cause
 * race conditions (it actually can be seen in the example and if we run the code without changing the Queue). I propose to enter
 * sessions object where we can guarantee the right order for processing messages by one worker per Item at a time.
 *
 * Also, I would propose not storing messages in an array here, because when we need to remove a specific message (during committing)
 * by using the splice function we can face race conditions as well. It would be better to use Map for representing our Queue.
 *
 * If we need to use somehow a relation between the processing Item key and the corresponding Worker we can store the workerId as well.
 * On the current requirements stage, I don't see a reason to do that.
 *
 * It would be nice to allow the Worker to process several messages from different sessions. We could increase performance and possibly
 * reduce the number of Workers to process all messages. In the current requirements stage, a worker is limited to processing only one message at a time.
 *
 * I feel that the Queue could already handle such an improvement with the Worker class.
 */
export class Queue {
    private messages = new Map<Message['id'], Message>();
    private sessions = new Set<Message['key']>();

    Enqueue(message: Message) {
        this.messages.set(message.id, message);
    }

    Dequeue(workerId: number): Message | undefined {
        const message = [...this.messages.values()].find((m) => !this.sessions.has(m.key));

        if (message) {
            this.sessions.add(message.key);
        }

        return message;
    }

    Confirm(workerId: number, messageId: string) {
        const message = this.messages.get(messageId);

        if (!message) {
            return;
        }

        this.messages.delete(messageId);
        this.sessions.delete(message.key);
    }

    Size () {
        return this.messages.size
    }
}

