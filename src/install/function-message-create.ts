import { MessageCreateResultCode, MessageEventType } from "@src/core/constant"
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
                    p_channel TEXT,
                    p_content BYTEA,
                    p_dequeue_at BIGINT
                ) RETURNS TABLE (
                    result_code INTEGER,
                    metadata JSON
                ) AS $$
                DECLARE
                    v_now BIGINT;
                    v_dequeue_at BIGINT;
                    v_channel_policy RECORD;
                    v_channel_state RECORD;
                    v_message RECORD;
                BEGIN
                    v_now := ${ref(params.schema)}."epoch"();
                    v_dequeue_at := COALESCE(p_dequeue_at, v_now);

                    SELECT 
                        "id",
                        "current_concurrency",
                        "current_size",
                        "max_concurrency",
                        "max_size",
                        "release_interval_ms",
                        "dequeue_prev_at",
                        "message_id",
                        "message_dequeue_at"
                    FROM ${ref(params.schema)}."channel_state"
                    WHERE "id" = p_channel
                    FOR UPDATE
                    INTO v_channel_state;

                    IF v_channel_state."id" IS NULL THEN
                        SELECT
                            "channel_policy"."max_concurrency",
                            "channel_policy"."max_size",
                            "channel_policy"."release_interval_ms"
                        FROM ${ref(params.schema)}."channel_policy"
                        WHERE "id" = p_channel
                        FOR SHARE
                        INTO v_channel_policy;

                        INSERT INTO ${ref(params.schema)}."channel_state" (
                            "id",
                            "current_concurrency",
                            "current_size",
                            "max_concurrency",
                            "max_size",
                            "release_interval_ms",
                            "dequeue_prev_at"
                        ) VALUES (
                            p_channel,
                            0,
                            0,
                            v_channel_policy."max_concurrency",
                            v_channel_policy."max_size",
                            v_channel_policy."release_interval_ms",
                            v_now
                        ) RETURNING
                            "id",
                            "current_concurrency",
                            "current_size",
                            "max_concurrency",
                            "max_size",
                            "release_interval_ms",
                            "dequeue_prev_at",
                            "message_id",
                            "message_dequeue_at"
                        INTO v_channel_state;
                    END IF;

                    IF v_channel_state."current_size" >= v_channel_state."max_size" THEN
                        RETURN QUERY SELECT
                            ${value(MessageCreateResultCode.MESSAGE_DROPPED)},
                            NULL::JSON;
                        RETURN;
                    END IF;

                    INSERT INTO ${ref(params.schema)}."message" (
                        "channel_id",
                        "content",
                        "is_locked",
                        "num_attempts",
                        "dequeue_at"
                    ) VALUES (
                        p_channel,
                        p_content,
                        FALSE,
                        0,
                        v_dequeue_at
                    ) RETURNING
                        "id"
                    INTO v_message;

                    IF 
                        v_channel_state."message_id" IS NULL OR
                        v_dequeue_at < v_channel_state."message_dequeue_at" OR
                        v_dequeue_at = v_channel_state."message_dequeue_at" AND v_message."id" < v_channel_state."message_id"
                    THEN
                        UPDATE ${ref(params.schema)}."channel_state" SET
                            "current_size" = v_channel_state."current_size" + 1,
                            "message_id" = v_message."id",
                            "message_dequeue_at" = v_dequeue_at,
                            "dequeue_next_at" = GREATEST(
                                v_channel_state."dequeue_prev_at" + COALESCE(v_channel_state."release_interval_ms", 0),
                                v_dequeue_at
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
                                'id', v_message."id"::TEXT,
                                'dequeue_at', v_dequeue_at
                            )::TEXT
                        );
                    END IF;

                    RETURN QUERY SELECT
                        ${value(MessageCreateResultCode.MESSAGE_CREATED)},
                        JSON_BUILD_OBJECT(
                            'id', v_message."id"::TEXT,
                            'channel_size', v_channel_state."current_size" + 1
                        );
                END;
                $$ LANGUAGE plpgsql;
            `
        ]
    }
}
