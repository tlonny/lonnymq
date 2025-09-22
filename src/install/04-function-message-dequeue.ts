import { MessageDequeueResultCode } from "@src/core/constant"
import { pathNormalize } from "@src/core/path"
import { ref, sql, value, type SqlNode } from "@src/core/sql"

export const messageLockedDequeueQuery = (params : {
    schema: string,
    now: SqlNode,
}) => sql`
    SELECT
        "message"."id",
        "message"."state",
        "message"."content",
        "message"."channel_name",
        "message"."lock_ms",
        "message"."unlock_at",
        "message"."num_attempts"
    FROM ${ref(params.schema)}."message"
    WHERE "is_locked"
    AND "unlock_at" <= ${params.now}
    ORDER BY "unlock_at" ASC
`

export const channelDequeueQuery = (params : {
    schema: string
}) => sql`
    SELECT
        "channel_state"."id",
        "channel_state"."name",
        "channel_state"."release_interval_ms",
        "channel_state"."message_id",
        "channel_state"."active_next_at",
        "channel_state"."active_prev_at",
        "channel_state"."current_concurrency"
    FROM ${ref(params.schema)}."channel_state"
    WHERE "message_id" IS NOT NULL
    AND ("max_concurrency" IS NULL OR "current_concurrency" < "max_concurrency")
    ORDER BY "active_next_at" ASC
`

export const messageNextDequeueQuery = (params : {
    schema: string,
    channelName: SqlNode
}) => sql`
    SELECT
        "message"."id",
        "message"."dequeue_at",
        "message"."seq_no"
    FROM ${ref(params.schema)}."message"
    WHERE NOT "is_locked"
    AND "channel_name" = ${params.channelName}
    ORDER BY "dequeue_at" ASC, "seq_no" ASC
`

export const installFunctionMessageDequeue = {
    name: pathNormalize(__filename),
    sql: (params : {
        schema: string
    }) => {
        const messageLockedDequeue = messageLockedDequeueQuery({
            now: sql`v_now`,
            schema: params.schema,
        })

        const messageNextDequeue = messageNextDequeueQuery({
            channelName: sql`v_channel_state."name"`,
            schema: params.schema,
        })

        const channelDequeue = channelDequeueQuery({
            schema: params.schema,
        })

        return [
            sql`
                CREATE FUNCTION ${ref(params.schema)}."message_dequeue" ()
                RETURNS TABLE (
                    result_code INTEGER,
                    content BYTEA,
                    state BYTEA,
                    metadata JSON
                ) AS $$
                DECLARE
                    v_now TIMESTAMP;
                    v_channel_state RECORD;
                    v_message_locked RECORD;
                    v_message_dequeue RECORD;
                    v_message_next RECORD;
                BEGIN
                    v_now := NOW();

                    ${messageLockedDequeue}
                    FOR UPDATE
                    SKIP LOCKED
                    LIMIT 1
                    INTO v_message_locked;

                    IF v_message_locked."id" IS NOT NULL THEN
                        UPDATE ${ref(params.schema)}."message" SET
                            "num_attempts" = v_message_locked."num_attempts" + 1,
                            "unlock_at" = v_now + (v_message_locked."lock_ms" * INTERVAL '1 millisecond')
                        WHERE "id" = v_message_locked."id";

                        RETURN QUERY SELECT 
                            ${value(MessageDequeueResultCode.MESSAGE_DEQUEUED)},
                            v_message_locked.content,
                            v_message_locked.state,
                            JSON_BUILD_OBJECT(
                                'id', v_message_locked.id,
                                'is_unlocked', TRUE,
                                'channel_name', v_message_locked.channel_name,
                                'num_attempts', v_message_locked.num_attempts + 1
                            );
                        RETURN;
                    END IF;

                    ${channelDequeue}
                    FOR UPDATE
                    SKIP LOCKED
                    LIMIT 1
                    INTO v_channel_state;

                    IF v_channel_state."id" IS NULL THEN
                        RETURN QUERY SELECT
                            ${value(MessageDequeueResultCode.MESSAGE_NOT_AVAILABLE)},
                            NULL::BYTEA,
                            NULL::BYTEA,
                            JSON_BUILD_OBJECT('retry_ms', NULL);
                        RETURN;
                    END IF;

                    IF v_channel_state."active_next_at" > v_now THEN
                        RETURN QUERY SELECT
                            ${value(MessageDequeueResultCode.MESSAGE_NOT_AVAILABLE)},
                            NULL::BYTEA,
                            NULL::BYTEA,
                            JSON_BUILD_OBJECT(
                                'retry_ms', CEIL(1_000 * EXTRACT(EPOCH FROM v_channel_state."active_next_at" - v_now))
                            );
                        RETURN;
                    END IF;

                    SELECT
                        "message"."id",
                        "message"."channel_name",
                        "message"."content",
                        "message"."num_attempts",
                        "message"."state",
                        "message"."lock_ms"
                    FROM ${ref(params.schema)}."message"
                    WHERE "id" = v_channel_state."message_id"
                    INTO v_message_dequeue;

                    UPDATE ${ref(params.schema)}."message" SET
                        "is_locked" = TRUE,
                        "num_attempts" = v_message_dequeue."num_attempts" + 1,
                        "unlock_at" = v_now + (v_message_dequeue."lock_ms" * INTERVAL '1 millisecond')
                    WHERE "id" = v_message_dequeue."id";

                    ${messageNextDequeue}
                    LIMIT 1
                    INTO v_message_next;

                    IF v_message_next."id" IS NULL THEN
                        UPDATE ${ref(params.schema)}."channel_state" SET
                            "current_concurrency" = v_channel_state."current_concurrency" + 1,
                            "active_prev_at" = v_now,
                            "message_id" = NULL
                        WHERE "id" = v_channel_state."id";
                    ELSE
                        UPDATE ${ref(params.schema)}."channel_state" SET
                            "current_concurrency" = v_channel_state."current_concurrency" + 1,
                            "message_id" = v_message_next."id",
                            "message_dequeue_at" = v_message_next."dequeue_at",
                            "message_seq_no" = v_message_next."seq_no",
                            "active_prev_at" = v_now,
                            "active_next_at" = GREATEST(
                                v_message_next."dequeue_at",
                                v_now + (COALESCE(v_channel_state."release_interval_ms", 0) * INTERVAL '1 millisecond')
                            )
                        WHERE "id" = v_channel_state."id";
                    END IF;

                    RETURN QUERY SELECT
                        ${value(MessageDequeueResultCode.MESSAGE_DEQUEUED)},
                        v_message_dequeue.content,
                        v_message_dequeue.state,
                        JSON_BUILD_OBJECT(
                            'id', v_message_dequeue.id,
                            'is_unlocked', FALSE,
                            'channel_name', v_message_dequeue.channel_name,
                            'num_attempts', v_message_dequeue.num_attempts + 1
                        );
                    RETURN;
                END;
                $$ LANGUAGE plpgsql;
            `
        ]
    }
}
