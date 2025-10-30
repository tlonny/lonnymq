import { MessageDeferResultCode, MessageEventType } from "@src/core/constant"
import { pathNormalize } from "@src/core/path"
import { ref, sql, value } from "@src/core/sql"

export const installFunctionMessageDefer = {
    name: pathNormalize(__filename),
    sql: (params : {
        schema: string,
        eventChannel: string | null,
    }) => {
        return [
            sql`
                CREATE FUNCTION ${ref(params.schema)}."message_defer" (
                    p_id BIGINT,
                    p_num_attempts BIGINT,
                    p_dequeue_at BIGINT,
                    p_state BYTEA
                )
                RETURNS TABLE (
                    result_code INTEGER
                ) AS $$
                DECLARE
                    v_now BIGINT;
                    v_channel_state RECORD;
                    v_message RECORD;
                    v_dequeue_at BIGINT;
                BEGIN
                    v_now := ${ref(params.schema)}."epoch"();

                    SELECT
                        "message"."id",
                        "message"."channel_id",
                        "message"."num_attempts",
                        "message"."is_locked"
                    FROM ${ref(params.schema)}."message"
                    WHERE "id" = p_id
                    FOR UPDATE
                    INTO v_message;

                    IF v_message."id" IS NULL THEN
                        RETURN QUERY SELECT
                            ${value(MessageDeferResultCode.MESSAGE_NOT_FOUND)};
                        RETURN;
                    ELSIF NOT v_message."is_locked" OR v_message."num_attempts" <> p_num_attempts THEN
                        RETURN QUERY SELECT
                            ${value(MessageDeferResultCode.MESSAGE_STATE_INVALID)};
                        RETURN;
                    END IF;

                    SELECT
                        "channel_state"."current_concurrency",
                        "channel_state"."release_interval_ms",
                        "channel_state"."message_id",
                        "channel_state"."message_dequeue_at",
                        "channel_state"."dequeue_prev_at"
                    FROM ${ref(params.schema)}."channel_state"
                    WHERE "id" = v_message."channel_id"
                    FOR UPDATE
                    INTO v_channel_state;

                    v_dequeue_at := COALESCE(p_dequeue_at, v_now);

                    IF 
                        v_channel_state."message_id" IS NULL OR 
                        v_dequeue_at < v_channel_state."message_dequeue_at" OR
                        v_dequeue_at = v_channel_state."message_dequeue_at" AND v_message."id" < v_channel_state."message_id"
                    THEN
                        UPDATE ${ref(params.schema)}."channel_state" SET
                            "current_concurrency" = v_channel_state."current_concurrency" - 1,
                            "message_id" = v_message."id",
                            "message_dequeue_at" = v_dequeue_at,
                            "dequeue_next_at" = GREATEST(
                                v_channel_state."dequeue_prev_at" + COALESCE(v_channel_state."release_interval_ms", 0),
                                v_dequeue_at
                            )
                        WHERE "id" = v_message."channel_id";
                    ELSE
                        UPDATE ${ref(params.schema)}."channel_state" SET
                            "current_concurrency" = v_channel_state."current_concurrency" - 1
                        WHERE "id" = v_message."channel_id";
                    END IF;

                    UPDATE ${ref(params.schema)}."message" SET
                        "state" = p_state,
                        "is_locked" = FALSE,
                        "dequeue_at" = v_dequeue_at
                    WHERE "id" = p_id;

                    IF ${value(params.eventChannel !== null)} THEN
                        PERFORM PG_NOTIFY(
                            ${value(params.eventChannel)},
                            JSON_BUILD_OBJECT(
                                'type', ${value(MessageEventType.MESSAGE_DEFERRED)},
                                'dequeue_at', v_dequeue_at,
                                'id', p_id::TEXT
                            )::TEXT
                        );
                    END IF;

                    RETURN QUERY SELECT
                        ${value(MessageDeferResultCode.MESSAGE_DEFERRED)};
                    RETURN;
                END;
                $$ LANGUAGE plpgsql;
            `
        ]
    }
}
