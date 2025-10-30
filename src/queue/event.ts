import { MessageEventType } from "@src/core/constant"

type QueueEventPayloadMessageCreate = {
    type: MessageEventType.MESSAGE_CREATED
    id: string,
    dequeue_at: string
}

type QueueEventPayloadMessageDelete = {
    type: MessageEventType.MESSAGE_DELETED,
    id: string,
}

type QueueEventPayloadMessageDefer = {
    type: MessageEventType.MESSAGE_DEFERRED,
    id: string,
    dequeue_at: string
}

type QueueEventPayload =
    | QueueEventPayloadMessageCreate
    | QueueEventPayloadMessageDelete
    | QueueEventPayloadMessageDefer

export type QueueEventMessageCreate = {
    eventType: "MESSAGE_CREATED",
    id: string,
    dequeueAt: number
}

export type QueueEventMessageDeleted = {
    eventType: "MESSAGE_DELETED",
    id: string,
}

export type QueueEventMessageDeferred = {
    eventType: "MESSAGE_DEFERRED",
    id: string,
    dequeueAt: number
}

export type QueueEvent =
    | QueueEventMessageCreate
    | QueueEventMessageDeleted
    | QueueEventMessageDeferred

export const queueEventDecode = (payload : string) : QueueEvent => {
    const parsed = JSON.parse(payload) as QueueEventPayload
    if (parsed.type === MessageEventType.MESSAGE_CREATED) {
        return {
            eventType: "MESSAGE_CREATED",
            id: parsed.id,
            dequeueAt: Number(parsed.dequeue_at),
        }
    } else if (parsed.type === MessageEventType.MESSAGE_DELETED) {
        return {
            eventType: "MESSAGE_DELETED",
            id: parsed.id,
        }
    } else if (parsed.type === MessageEventType.MESSAGE_DEFERRED) {
        return {
            eventType: "MESSAGE_DEFERRED",
            id: parsed.id,
            dequeueAt: Number(parsed.dequeue_at),
        }
    } else {
        parsed satisfies never
        throw new Error("Unknown event type")
    }
}
