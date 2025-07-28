import { pathNormalize } from "@src/core/path"
import { ref, sql, value } from "@src/core/sql"

export enum MessageDequeueResultCode {
    MESSAGE_NOT_AVAILABLE,
    MESSAGE_DEQUEUED
}

export const migrationFunctionMessageDequeue = {
    name: pathNormalize(__filename),
    sql: (params : {
        schema: string,
    }) => {
        return [
            sql`
                CREATE FUNCTION ${ref(params.schema)}."message_dequeue" ()
                RETURNS JSONB AS $$
                DECLARE
                    v_now TIMESTAMP;
                    v_dequeue_id UUID;
                    v_channel_state RECORD;
                    v_message_locked RECORD;
                    v_retry_after TIMESTAMP;
                    v_message_dequeue RECORD;
                    v_message_next_dequeue RECORD;
                    v_message_next_dequeue_after TIMESTAMP;
                BEGIN
                    v_now := NOW();
                    v_dequeue_id := GEN_RANDOM_UUID();

                    SELECT
                        "id",
                        "name",
                        "state",
                        "content",
                        "channel_name",
                        "lock_ms",
                        "dequeue_after",
                        "num_attempts"
                    FROM ${ref(params.schema)}."message"
                    WHERE "is_locked"
                    ORDER BY "dequeue_after" ASC
                    FOR UPDATE
                    SKIP LOCKED
                    LIMIT 1
                    INTO v_message_locked;

                    IF v_message_locked."dequeue_after" <= v_now THEN
                        UPDATE ${ref(params.schema)}."message" SET
                            "num_attempts" = v_message_locked."num_attempts" + 1,
                            "dequeue_after" = v_now + (v_message_locked."lock_ms" * INTERVAL '1 millisecond'),
                            "dequeue_id" = v_dequeue_id
                        WHERE "id" = v_message_locked."id";

                        PERFORM ${ref(params.schema)}."wake"(v_message_locked."lock_ms");

                        RETURN JSONB_BUILD_OBJECT(
                            'result_code', ${value(MessageDequeueResultCode.MESSAGE_DEQUEUED)},
                            'id', v_message_locked.id,
                            'channel_name', v_message_locked.channel_name,
                            'state', v_message_locked.state,
                            'name', v_message_locked.name,
                            'dequeue_id', v_dequeue_id,
                            'content', v_message_locked.content,
                            'num_attempts', v_message_locked.num_attempts
                        );
                    END IF;

                    SELECT
                        "id",
                        "name",
                        "message_next_id",
                        "message_next_dequeue_after",
                        "current_concurrency"
                    FROM ${ref(params.schema)}."channel_state"
                    WHERE "message_next_id" IS NOT NULL
                    AND ("max_concurrency" IS NULL OR "current_concurrency" < "max_concurrency")
                    ORDER BY "message_next_dequeue_after" ASC
                    FOR UPDATE
                    SKIP LOCKED
                    LIMIT 1
                    INTO v_channel_state;

                    IF v_channel_state."id" IS NULL OR v_channel_state."message_next_dequeue_after" > v_now THEN
                        v_retry_after := LEAST(
                            v_channel_state."message_next_dequeue_after",
                            v_message_locked."dequeue_after"
                        );

                        RETURN JSONB_BUILD_OBJECT(
                            'result_code', ${value(MessageDequeueResultCode.MESSAGE_NOT_AVAILABLE)},
                            'retry_ms', CEIL(EXTRACT(MILLISECOND FROM v_retry_after - v_now))
                        );
                    END IF;

                    SELECT
                        "id",
                        "name",
                        "channel_name",
                        "content",
                        "num_attempts",
                        "state",
                        "lock_ms"
                    FROM ${ref(params.schema)}."message"
                    WHERE "id" = v_channel_state."message_next_id"
                    INTO v_message_dequeue;

                    UPDATE ${ref(params.schema)}."message" SET
                        "is_locked" = TRUE,
                        "num_attempts" = v_message_dequeue."num_attempts" + 1,
                        "dequeue_id" = v_dequeue_id,
                        "dequeue_after" = v_now + (v_message_dequeue."lock_ms" * INTERVAL '1 millisecond')
                    WHERE "id" = v_message_dequeue."id";

                    PERFORM ${ref(params.schema)}."wake"(v_message_dequeue."lock_ms");

                    SELECT
                        "id",
                        "dequeue_after"
                    FROM ${ref(params.schema)}."message"
                    WHERE NOT "is_locked"
                    AND "channel_name" = v_message_dequeue."channel_name"
                    ORDER BY "dequeue_after" ASC
                    LIMIT 1
                    INTO v_message_next_dequeue;

                    IF v_message_next_dequeue."id" IS NOT NULL THEN
                        v_message_next_dequeue_after := GREATEST(
                            v_message_next_dequeue."dequeue_after",
                            v_now
                        );

                        UPDATE ${ref(params.schema)}."channel_state" SET
                            "current_concurrency" = v_channel_state."current_concurrency" + 1,
                            "message_next_id" = v_message_next_dequeue."id",
                            "message_next_dequeue_after" = v_message_next_dequeue_after
                        WHERE "id" = v_channel_state."id";
                    ELSE
                        UPDATE ${ref(params.schema)}."channel_state" SET
                            "current_concurrency" = v_channel_state."current_concurrency" + 1,
                            "message_next_id" = NULL
                        WHERE "id" = v_channel_state."id";
                    END IF;


                    RETURN JSONB_BUILD_OBJECT(
                        'result_code', ${value(MessageDequeueResultCode.MESSAGE_DEQUEUED)},
                        'id', v_message_dequeue.id,
                        'channel_name', v_message_dequeue.channel_name,
                        'state', v_message_dequeue.state,
                        'dequeue_id', v_dequeue_id,
                        'name', v_message_dequeue.name,
                        'content', v_message_dequeue.content,
                        'num_attempts', v_message_dequeue.num_attempts
                    );
                END;
                $$ LANGUAGE plpgsql;
            `
        ]
    }
}
