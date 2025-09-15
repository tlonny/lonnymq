import { MessageHeartbeatResultCode } from "@src/core/constant"
import { pathNormalize } from "@src/core/path"
import { ref, sql, value } from "@src/core/sql"

export const migrationFunctionMessageHeartbeat = {
    name: pathNormalize(__filename),
    sql: (params : {
        schema: string,
        eventChannel: string | null,
    }) => {
        return [
            sql`
                CREATE FUNCTION ${ref(params.schema)}."message_heartbeat" (
                    p_id UUID,
                    p_num_attempts BIGINT
                )
                RETURNS TABLE (
                    result_code INTEGER
                ) AS $$
                DECLARE
                    v_now TIMESTAMP;
                    v_message RECORD;
                BEGIN
                    v_now := NOW();

                    SELECT
                        "message"."id",
                        "message"."is_locked",
                        "message"."num_attempts",
                        "message"."lock_ms"
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
                        "unlock_at" = v_now + INTERVAL '1 MILLISECOND' * v_message."lock_ms"
                    WHERE "id" = p_id;

                    RETURN QUERY SELECT
                        ${value(MessageHeartbeatResultCode.MESSAGE_HEARTBEATEDED)};
                    RETURN;
                END;
                $$ LANGUAGE plpgsql;
            `
        ]
    }
}
