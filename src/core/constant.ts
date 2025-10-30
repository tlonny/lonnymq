export enum MessageCreateResultCode {
    MESSAGE_CREATED,
    MESSAGE_DROPPED
}

export enum MessageDequeueResultCode {
    MESSAGE_NOT_AVAILABLE,
    MESSAGE_DEQUEUED
}

export enum MessageDeleteResultCode {
    MESSAGE_NOT_FOUND,
    MESSAGE_STATE_INVALID,
    MESSAGE_DELETED
}

export enum MessageDeferResultCode {
    MESSAGE_NOT_FOUND,
    MESSAGE_STATE_INVALID,
    MESSAGE_DEFERRED
}

export enum MessageHeartbeatResultCode {
    MESSAGE_NOT_FOUND,
    MESSAGE_STATE_INVALID,
    MESSAGE_HEARTBEATED
}

export enum MessageEventType {
    MESSAGE_CREATED,
    MESSAGE_DELETED,
    MESSAGE_DEFERRED,
}
