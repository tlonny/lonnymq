import { pathNormalize } from "@src/core/path"
import { ref, sql, value } from "@src/core/sql"

export enum MessageDeleteResultCode {
    MESSAGE_NOT_FOUND,
    MESSAGE_STATE_INVALID,
    MESSAGE_DELETED
}

export const migrationFunctionMessageDelete = {
    name: pathNormalize(__filename),
    sql: (params : {
        schema: string,
    }) => {
        return [
            sql`
                CREATE FUNCTION ${ref(params.schema)}."message_delete" (
                    p_id UUID,
                    p_dequeue_id UUID
                )
                RETURNS JSONB AS $$
                DECLARE
                    v_channel_state RECORD;
                    v_message RECORD;
                BEGIN
                    SELECT
                        "id",
                        "channel_name",
                        "dequeue_id"
                    FROM ${ref(params.schema)}."message"
                    WHERE "id" = p_id
                    FOR UPDATE
                    INTO v_message;

                    IF v_message."id" IS NULL THEN
                        RETURN JSONB_BUILD_OBJECT(
                            'result_code', ${value(MessageDeleteResultCode.MESSAGE_NOT_FOUND)}
                        );
                    ELSEIF v_message."dequeue_id" != p_dequeue_id THEN
                        RETURN JSONB_BUILD_OBJECT(
                            'result_code', ${value(MessageDeleteResultCode.MESSAGE_STATE_INVALID)}
                        );
                    END IF;

                    SELECT
                        "id",
                        "current_size",
                        "current_concurrency"
                    FROM ${ref(params.schema)}."channel_state"
                    WHERE "name" = v_message."channel_name"
                    FOR UPDATE
                    INTO v_channel_state;

                    IF v_channel_state."current_size" = 1 THEN
                        DELETE FROM ${ref(params.schema)}."channel_state"
                        WHERE "id" = v_channel_state."id";
                    ELSE
                        UPDATE ${ref(params.schema)}."channel_state" SET
                            "current_concurrency" = v_channel_state."current_concurrency" - 1,
                            "current_size" = v_channel_state."current_size" - 1
                        WHERE "id" = v_channel_state."id";
                    END IF;

                    DELETE FROM ${ref(params.schema)}."message"
                    WHERE "id" = p_id;

                    RETURN JSONB_BUILD_OBJECT(
                        'result_code', ${value(MessageDeleteResultCode.MESSAGE_DELETED)}
                    );
                END;
                $$ LANGUAGE plpgsql;
            `
        ]
    }
}
