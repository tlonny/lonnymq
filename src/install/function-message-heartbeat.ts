import { MessageHeartbeatResultCode } from "@src/core/constant"
import { pathNormalize } from "@src/core/path"
import { ref, sql, value } from "@src/core/sql"

export const installFunctionMessageHeartbeat = {
    name: pathNormalize(__filename),
    sql: (params : {
        schema: string,
        eventChannel: string | null,
    }) => {
        return [
            sql`
                CREATE FUNCTION ${ref(params.schema)}."message_heartbeat" (
                    p_id BIGINT,
                    p_num_attempts BIGINT,
                    p_lock_ms BIGINT
                )
                RETURNS TABLE (
                    result_code INTEGER
                ) AS $$
                DECLARE
                    v_now BIGINT;
                    v_message RECORD;
                BEGIN
                    v_now := ${ref(params.schema)}."epoch"();

                    SELECT
                        "message"."id",
                        "message"."num_attempts",
                        "message"."is_locked",
                        "message"."unlock_at"
                    FROM ${ref(params.schema)}."message"
                    WHERE "id" = p_id
                    FOR UPDATE
                    INTO v_message;

                    IF v_message."id" IS NULL THEN
                        RETURN QUERY SELECT
                            ${value(MessageHeartbeatResultCode.MESSAGE_NOT_FOUND)};
                        RETURN;
                    ELSIF NOT v_message."is_locked" OR v_message."num_attempts" <> p_num_attempts THEN
                        RETURN QUERY SELECT
                            ${value(MessageHeartbeatResultCode.MESSAGE_STATE_INVALID)};
                        RETURN;
                    END IF;

                    UPDATE ${ref(params.schema)}."message" SET
                        "unlock_at" = GREATEST(
                            v_now + p_lock_ms,
                            v_message."unlock_at"
                        )
                    WHERE "id" = p_id;

                    RETURN QUERY SELECT
                        ${value(MessageHeartbeatResultCode.MESSAGE_HEARTBEATED)};
                    RETURN;
                END;
                $$ LANGUAGE plpgsql;
            `
        ]
    }
}
