import { MessageEventType } from "@src/core/constant"
import { pathNormalize } from "@src/core/path"
import { ref, sql, value } from "@src/core/sql"

export const installFunctionMessageCreate = {
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
                    p_content BYTEA,
                    p_delay_ms BIGINT
                ) RETURNS VOID AS $$
                DECLARE
                    v_now TIMESTAMP;
                    v_channel_policy RECORD;
                    v_channel_state RECORD;
                    v_message RECORD;
                BEGIN
                    v_now := NOW();

                    INSERT INTO ${ref(params.schema)}."message" (
                        "id",
                        "channel_name",
                        "content",
                        "is_locked",
                        "num_attempts",
                        "dequeue_at",
                        "created_at"
                    ) VALUES (
                        p_id,
                        p_channel_name,
                        p_content,
                        FALSE,
                        0,
                        v_now + INTERVAL '1 MILLISECOND' * p_delay_ms,
                        v_now
                    ) RETURNING
                        "id", 
                        "seq_no",
                        "dequeue_at"
                    INTO v_message;

                    SELECT
                        "channel_policy"."max_concurrency",
                        "channel_policy"."release_interval_ms"
                    FROM ${ref(params.schema)}."channel_policy"
                    WHERE "name" = p_channel_name
                    FOR SHARE
                    INTO v_channel_policy;

                    INSERT INTO ${ref(params.schema)}."channel_state" (
                        "name",
                        "current_concurrency",
                        "current_size",
                        "max_concurrency",
                        "release_interval_ms",
                        "active_prev_at",
                        "created_at"
                    ) VALUES (
                        p_channel_name,
                        0,
                        0,
                        v_channel_policy."max_concurrency",
                        v_channel_policy."release_interval_ms",
                        v_now,
                        v_now
                    ) ON CONFLICT ("name") 
                    DO UPDATE SET "name" = EXCLUDED."name"
                    RETURNING
                        "id",
                        "current_concurrency",
                        "current_size",
                        "max_concurrency",
                        "release_interval_ms",
                        "active_prev_at",
                        "message_id",
                        "message_dequeue_at",
                        "message_seq_no"
                    INTO v_channel_state;

                    IF 
                        v_channel_state."message_id" IS NULL OR
                        v_message."dequeue_at" < v_channel_state."message_dequeue_at" OR
                        v_message."dequeue_at" = v_channel_state."message_dequeue_at" AND v_message."seq_no" < v_channel_state."message_seq_no"
                    THEN
                        UPDATE ${ref(params.schema)}."channel_state" SET
                            "current_size" = v_channel_state."current_size" + 1,
                            "message_id" = v_message."id",
                            "message_dequeue_at" = v_message."dequeue_at",
                            "message_seq_no" = v_message."seq_no",
                            "active_next_at" = GREATEST(
                                v_channel_state."active_prev_at" + INTERVAL '1 MILLISECOND' * COALESCE(v_channel_state."release_interval_ms", 0),
                                v_message."dequeue_at"
                            )
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
                END;
                $$ LANGUAGE plpgsql;
            `
        ]
    }
}
