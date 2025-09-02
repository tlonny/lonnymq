import { pathNormalize } from "@src/core/path"
import { ref, sql, value } from "@src/core/sql"

export enum MessageDeferResultCode {
    MESSAGE_NOT_FOUND,
    MESSAGE_STATE_INVALID,
    MESSAGE_DEFERRED
}

export const migrationFunctionMessageDefer = {
    name: pathNormalize(__filename),
    sql: (params : {
        schema: string,
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
                    v_channel_state RECORD;
                    v_dequeue_after TIMESTAMP;
                    v_message RECORD;
                BEGIN
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
                        "channel_state"."message_next_id",
                        "channel_state"."message_next_dequeue_after",
                        "channel_state"."message_next_seq_no"
                    FROM ${ref(params.schema)}."channel_state"
                    WHERE "name" = v_message."channel_name"
                    FOR UPDATE
                    INTO v_channel_state;

                    v_dequeue_after := NOW() + INTERVAL '1 MILLISECOND' * p_delay_ms;

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

                    PERFORM ${ref(params.schema)}."wake"(GREATEST(0, p_delay_ms));

                        RETURN QUERY SELECT
                            ${value(MessageDeferResultCode.MESSAGE_DEFERRED)};
                        RETURN;
                END;
                $$ LANGUAGE plpgsql;
            `
        ]
    }
}
