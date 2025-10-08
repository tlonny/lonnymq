import { MessageEventType } from "@src/core/constant"

type QueueEventPayloadMessageCreate = {
    type: MessageEventType.MESSAGE_CREATED
    id: string,
    delay_ms: number
}

type QueueEventPayloadMessageDelete = {
    type: MessageEventType.MESSAGE_DELETED,
    id: string,
}

type QueueEventPayloadMessageDefer = {
    type: MessageEventType.MESSAGE_DEFERRED,
    id: string,
    delay_ms: number
}

type QueueEventPayload =
    | QueueEventPayloadMessageCreate
    | QueueEventPayloadMessageDelete
    | QueueEventPayloadMessageDefer

type QueueEventMessageCreate = {
    eventType: "MESSAGE_CREATED",
    id: string,
    delayMs: number
}

type QueueEventMessageDeleted = {
    eventType: "MESSAGE_DELETED",
    id: string,
}

type QueueEventMessageDeferred = {
    eventType: "MESSAGE_DEFERRED",
    id: string,
    delayMs: number
}

type QueueEvent =
    | QueueEventMessageCreate
    | QueueEventMessageDeleted
    | QueueEventMessageDeferred

export const queueEventDecode = (payload : string) : QueueEvent => {
    const parsed = JSON.parse(payload) as QueueEventPayload
    if (parsed.type === MessageEventType.MESSAGE_CREATED) {
        return {
            eventType: "MESSAGE_CREATED",
            id: parsed.id,
            delayMs: parsed.delay_ms,
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
            delayMs: parsed.delay_ms,
        }
    } else {
        parsed satisfies never
        throw new Error("Unknown event type")
    }
}
