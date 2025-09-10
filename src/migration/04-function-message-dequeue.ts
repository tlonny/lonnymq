import { MessageDequeueResultCode } from "@src/core/constant"
import { pathNormalize } from "@src/core/path"
import { ref, sql, value } from "@src/core/sql"

export const migrationFunctionMessageDequeue = {
    name: pathNormalize(__filename),
    sql: (params : {
        schema: string
    }) => {
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
                    v_retry_after TIMESTAMP;
                    v_message_dequeue RECORD;
                    v_message_next_dequeue RECORD;
                    v_message_next_dequeue_after TIMESTAMP;
                BEGIN
                    v_now := NOW();

                    SELECT
                        "message"."id",
                        "message"."name",
                        "message"."state",
                        "message"."content",
                        "message"."channel_name",
                        "message"."lock_ms",
                        "message"."dequeue_after",
                        "message"."num_attempts"
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
                            "dequeue_after" = v_now + (v_message_locked."lock_ms" * INTERVAL '1 millisecond')
                        WHERE "id" = v_message_locked."id";

                        RETURN QUERY SELECT 
                            ${value(MessageDequeueResultCode.MESSAGE_DEQUEUED)},
                            v_message_locked.content,
                            v_message_locked.state,
                            JSON_BUILD_OBJECT(
                                'id', v_message_locked.id,
                                'channel_name', v_message_locked.channel_name,
                                'name', v_message_locked.name,
                                'num_attempts', v_message_locked.num_attempts
                            );
                        RETURN;
                    END IF;

                    SELECT
                        "channel_state"."id",
                        "channel_state"."name",
                        "channel_state"."release_interval_ms",
                        "channel_state"."message_next_id",
                        "channel_state"."message_next_dequeue_after",
                        "channel_state"."current_concurrency"
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

                        RETURN QUERY SELECT
                            ${value(MessageDequeueResultCode.MESSAGE_NOT_AVAILABLE)},
                            NULL::BYTEA,
                            NULL::BYTEA,
                            JSON_BUILD_OBJECT(
                                'retry_ms', CEIL(EXTRACT(MILLISECOND FROM v_retry_after - v_now))
                            );
                        RETURN;
                    END IF;

                    SELECT
                        "message"."id",
                        "message"."name",
                        "message"."channel_name",
                        "message"."content",
                        "message"."num_attempts",
                        "message"."state",
                        "message"."lock_ms"
                    FROM ${ref(params.schema)}."message"
                    WHERE "id" = v_channel_state."message_next_id"
                    INTO v_message_dequeue;

                    UPDATE ${ref(params.schema)}."message" SET
                        "is_locked" = TRUE,
                        "num_attempts" = v_message_dequeue."num_attempts" + 1,
                        "dequeue_after" = v_now + (v_message_dequeue."lock_ms" * INTERVAL '1 millisecond')
                    WHERE "id" = v_message_dequeue."id";

                    SELECT
                        "message"."id",
                        "message"."dequeue_after",
                        "message"."seq_no"
                    FROM ${ref(params.schema)}."message"
                    WHERE NOT "is_locked"
                    AND "channel_name" = v_message_dequeue."channel_name"
                    ORDER BY "dequeue_after" ASC, "seq_no" ASC
                    LIMIT 1
                    INTO v_message_next_dequeue;

                    IF v_message_next_dequeue."id" IS NOT NULL THEN
                        v_message_next_dequeue_after := GREATEST(
                            v_message_next_dequeue."dequeue_after",
                            v_now + (COALESCE(v_channel_state."release_interval_ms", 0) * INTERVAL '1 millisecond')
                        );

                        UPDATE ${ref(params.schema)}."channel_state" SET
                            "current_concurrency" = v_channel_state."current_concurrency" + 1,
                            "message_next_id" = v_message_next_dequeue."id",
                            "message_next_dequeue_after" = v_message_next_dequeue_after,
                            "message_next_seq_no" = v_message_next_dequeue."seq_no",
                            "message_last_dequeued_at" = v_now
                        WHERE "id" = v_channel_state."id";
                    ELSE
                        UPDATE ${ref(params.schema)}."channel_state" SET
                            "current_concurrency" = v_channel_state."current_concurrency" + 1,
                            "message_last_dequeued_at" = v_now,
                            "message_next_id" = NULL
                        WHERE "id" = v_channel_state."id";
                    END IF;


                    RETURN QUERY SELECT
                        ${value(MessageDequeueResultCode.MESSAGE_DEQUEUED)},
                        v_message_dequeue.content,
                        v_message_dequeue.state,
                        JSON_BUILD_OBJECT(
                            'id', v_message_dequeue.id,
                            'channel_name', v_message_dequeue.channel_name,
                            'name', v_message_dequeue.name,
                            'num_attempts', v_message_dequeue.num_attempts
                        );
                    RETURN;
                END;
                $$ LANGUAGE plpgsql;
            `
        ]
    }
}
