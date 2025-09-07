import { MessageCreateResultCode, MessageEventType } from "@src/core/constant"
import { pathNormalize } from "@src/core/path"
import { ref, sql, value } from "@src/core/sql"

export const migrationFunctionMessageCreate = {
    name: pathNormalize(__filename),
    sql: (params : {
        schema: string,
        eventChannel: string | null,
    }) => {
        return [
            sql`
                CREATE FUNCTION ${ref(params.schema)}."message_create" (
                    p_id UUID,
                    p_channel_name TEXT,
                    p_name TEXT,
                    p_content BYTEA,
                    p_lock_ms INTEGER,
                    p_delay_ms INTEGER
                ) RETURNS TABLE (
                    result_code INTEGER
                ) AS $$
                DECLARE
                    v_now TIMESTAMP;
                    v_dequeue_after TIMESTAMP;
                    v_channel_policy RECORD;
                    v_channel_state RECORD;
                    v_message RECORD;
                BEGIN
                    v_now := NOW();

                    SELECT
                        "channel_policy"."max_size",
                        "channel_policy"."max_concurrency",
                        "channel_policy"."release_interval_ms"
                    FROM ${ref(params.schema)}."channel_policy"
                    WHERE "name" = p_channel_name
                    FOR SHARE
                    INTO v_channel_policy;

                    INSERT INTO ${ref(params.schema)}."channel_state" (
                        "name",
                        "current_size",
                        "current_concurrency",
                        "max_size",
                        "max_concurrency",
                        "release_interval_ms",
                        "message_next_id",
                        "message_next_dequeue_after"
                    ) VALUES (
                        p_channel_name,
                        0,
                        0,
                        v_channel_policy."max_size",
                        v_channel_policy."max_concurrency",
                        v_channel_policy."release_interval_ms",
                        NULL,
                        NULL
                    ) ON CONFLICT ("name") 
                    DO UPDATE SET "id" = EXCLUDED."id"
                    RETURNING
                        "id",
                        "current_size",
                        "current_concurrency",
                        "max_size",
                        "max_concurrency",
                        "release_interval_ms",
                        "message_last_dequeued_at",
                        "message_next_id",
                        "message_next_dequeue_after",
                        "message_next_seq_no"
                    INTO v_channel_state;

                    IF v_channel_state."current_size" >= v_channel_policy."max_size" THEN
                        RETURN QUERY SELECT
                            ${value(MessageCreateResultCode.MESSAGE_DROPPED)};
                        RETURN;
                    END IF;

                    INSERT INTO ${ref(params.schema)}."message" (
                        "id",
                        "channel_name",
                        "name",
                        "content",
                        "lock_ms",
                        "dequeue_after"
                    ) VALUES (
                        p_id,
                        p_channel_name,
                        p_name,
                        p_content,
                        p_lock_ms,
                        v_now + INTERVAL '1 MILLISECOND' * p_delay_ms
                    ) ON CONFLICT ("channel_name", "name") 
                    WHERE "num_attempts" = 0
                    DO UPDATE SET
                        "channel_name" = EXCLUDED."channel_name",
                        "name" = EXCLUDED."name"
                    RETURNING
                        "id", 
                        "seq_no",
                        "dequeue_after"
                    INTO v_message;

                    IF v_message."id" != p_id THEN
                        RETURN QUERY SELECT 
                            ${value(MessageCreateResultCode.MESSAGE_DEDUPLICATED)};
                        RETURN;
                    END IF;

                    v_dequeue_after := GREATEST(
                        v_now,
                        v_channel_state."message_last_dequeued_at"
                            + INTERVAL '1 MILLISECOND' * COALESCE(v_channel_state."release_interval_ms", 0),
                        v_message."dequeue_after"
                    );

                    IF 
                        v_channel_state."message_next_id" IS NULL OR
                        v_channel_state."message_next_dequeue_after" > v_dequeue_after OR
                        (v_channel_state."message_next_dequeue_after" = v_dequeue_after AND v_channel_state."message_next_seq_no" > v_message."seq_no")
                    THEN
                        UPDATE ${ref(params.schema)}."channel_state" SET
                            "current_size" = v_channel_state."current_size" + 1,
                            "message_next_id" = v_message."id",
                            "message_next_dequeue_after" = v_dequeue_after,
                            "message_next_seq_no" = v_message."seq_no"
                        WHERE "id" = v_channel_state."id";
                    ELSE
                        UPDATE ${ref(params.schema)}."channel_state" SET
                            "current_size" = v_channel_state."current_size" + 1
                        WHERE "id" = v_channel_state."id";
                    END IF;

                    IF ${value(params.eventChannel !== null)} THEN
                        PERFORM PG_NOTIFY(
                            ${value(params.eventChannel)},
                            JSON_BUILD_OBJECT(
                                'type', ${value(MessageEventType.MESSAGE_CREATED)},
                                'id', p_id,
                                'delay_ms', p_delay_ms
                            )::TEXT
                        );
                    END IF;

                    RETURN QUERY SELECT
                        ${value(MessageCreateResultCode.MESSAGE_CREATED)};
                    RETURN;
                END;
                $$ LANGUAGE plpgsql;
            `
        ]
    }
}
