import { MessageDeferResultCode, MessageEventType } from "@src/core/constant"
import { pathNormalize } from "@src/core/path"
import { ref, sql, value } from "@src/core/sql"

export const migrationFunctionMessageDefer = {
    name: pathNormalize(__filename),
    sql: (params : {
        schema: string,
        eventChannel: string | null,
    }) => {
        return [
            sql`
                CREATE FUNCTION ${ref(params.schema)}."message_defer" (
                    p_id UUID,
                    p_delay_ms INTEGER,
                    p_state BYTEA
                )
                RETURNS TABLE (
                    result_code INTEGER
                ) AS $$
                DECLARE
                    v_now TIMESTAMP;
                    v_channel_state RECORD;
                    v_dequeue_after TIMESTAMP;
                    v_message RECORD;
                BEGIN
                    v_now := NOW();

                    SELECT
                        "message"."id",
                        "message"."channel_name",
                        "message"."is_locked",
                        "message"."seq_no"
                    FROM ${ref(params.schema)}."message"
                    WHERE "id" = p_id
                    FOR UPDATE
                    INTO v_message;

                    IF v_message."id" IS NULL THEN
                        RETURN QUERY SELECT
                            ${value(MessageDeferResultCode.MESSAGE_NOT_FOUND)};
                        RETURN;
                    ELSIF NOT v_message."is_locked" THEN
                        RETURN QUERY SELECT
                            ${value(MessageDeferResultCode.MESSAGE_STATE_INVALID)};
                        RETURN;
                    END IF;

                    SELECT
                        "channel_state"."current_concurrency",
                        "channel_state"."release_interval_ms",
                        "channel_state"."message_next_id",
                        "channel_state"."message_next_dequeue_after",
                        "channel_state"."message_last_dequeued_at",
                        "channel_state"."message_next_seq_no"
                    FROM ${ref(params.schema)}."channel_state"
                    WHERE "name" = v_message."channel_name"
                    FOR UPDATE
                    INTO v_channel_state;

                    v_dequeue_after := NOW() + INTERVAL '1 MILLISECOND' * p_delay_ms;

                    v_dequeue_after := GREATEST(
                        v_now,
                        v_now + INTERVAL '1 MILLISECOND' * p_delay_ms,
                        v_channel_state."message_last_dequeued_at"
                    );

                    IF 
                        v_channel_state."message_next_id" IS NULL OR 
                        v_channel_state."message_next_dequeue_after" > v_dequeue_after OR
                        (v_channel_state."message_next_dequeue_after" = v_dequeue_after AND v_channel_state."message_next_seq_no" > v_message."seq_no")
                    THEN
                        UPDATE ${ref(params.schema)}."channel_state" SET
                            "current_concurrency" = v_channel_state."current_concurrency" - 1,
                            "message_next_id" = v_message."id",
                            "message_next_dequeue_after" = v_dequeue_after,
                            "message_next_seq_no" = v_message."seq_no"
                        WHERE "name" = v_message."channel_name";
                    ELSE
                        UPDATE ${ref(params.schema)}."channel_state" SET
                            "current_concurrency" = v_channel_state."current_concurrency" - 1
                        WHERE "name" = v_message."channel_name";
                    END IF;

                    UPDATE ${ref(params.schema)}."message" SET
                        "state" = p_state,
                        "is_locked" = FALSE,
                        "dequeue_after" = v_dequeue_after
                    WHERE "id" = p_id;

                    IF ${value(params.eventChannel !== null)} THEN
                        PERFORM PG_NOTIFY(
                            ${value(params.eventChannel)},
                            JSON_BUILD_OBJECT(
                                'type', ${value(MessageEventType.MESSAGE_DEFERRED)},
                                'delay_ms', p_delay_ms,
                                'id', p_id
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
