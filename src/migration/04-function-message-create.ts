import { pathNormalize } from "@src/core/path"
import { ref, sql, value } from "@src/core/sql"

export enum MessageCreateResultCode {
    MESSAGE_CREATED,
    MESSAGE_DROPPED,
    MESSAGE_DEDUPLICATED
}

export const migrationFunctionMessageCreate = {
    name: pathNormalize(__filename),
    sql: (params : {
        schema: string,
    }) => {
        return [
            sql`
                CREATE FUNCTION ${ref(params.schema)}."message_create" (
                    p_channel_name TEXT,
                    p_name TEXT,
                    p_content TEXT,
                    p_lock_ms BIGINT,
                    p_delay_ms BIGINT
                ) RETURNS JSONB AS $$
                DECLARE
                    v_now TIMESTAMP;
                    v_channel_policy RECORD;
                    v_channel_state RECORD;
                    v_message RECORD;
                    v_message_next_dequeue_after TIMESTAMP;
                BEGIN
                    v_now := NOW() + INTERVAL '1 MILLISECOND' * p_delay_ms;

                    SELECT
                        "max_size",
                        "max_concurrency"
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
                        "message_next_id",
                        "message_next_dequeue_after"
                    ) VALUES (
                        p_channel_name,
                        0,
                        0,
                        v_channel_policy."max_size",
                        v_channel_policy."max_concurrency",
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
                        "message_next_id",
                        "message_next_dequeue_after"
                    INTO v_channel_state;

                    IF v_channel_state."current_size" >= v_channel_policy."max_size" THEN
                        RETURN JSONB_BUILD_OBJECT(
                            'result_code', ${value(MessageCreateResultCode.MESSAGE_DROPPED)}
                        );
                    END IF;

                    INSERT INTO ${ref(params.schema)}."message" (
                        "channel_name",
                        "name",
                        "content",
                        "lock_ms",
                        "dequeue_after"
                    ) VALUES (
                        p_channel_name,
                        p_name,
                        p_content,
                        p_lock_ms,
                        v_now + INTERVAL '1 MILLISECOND' * p_delay_ms
                    ) ON CONFLICT ("channel_name", "name") 
                    WHERE "num_attempts" = 0
                    DO UPDATE SET
                        "id" = EXCLUDED."id"
                    RETURNING
                        "id", 
                        "dequeue_after"
                    INTO v_message;

                    IF v_message."id" IS NULL THEN
                        RETURN JSONB_BUILD_OBJECT(
                            'result_code', ${value(MessageCreateResultCode.MESSAGE_DEDUPLICATED)},
                            'id', v_message."id"
                        );
                    END IF;

                    IF 
                        v_channel_state."message_next_id" IS NULL OR
                        v_channel_state."message_next_dequeue_after" > v_message."dequeue_after"
                    THEN
                        UPDATE ${ref(params.schema)}."channel_state" SET
                            "current_size" = v_channel_state."current_size" + 1,
                            "message_next_id" = v_message."id",
                            "message_next_dequeue_after" = v_message."dequeue_after"
                        WHERE "id" = v_channel_state."id";
                    ELSE
                        UPDATE ${ref(params.schema)}."channel_state" SET
                            "current_size" = v_channel_state."current_size" + 1
                        WHERE "id" = v_channel_state."id";
                    END IF;

                    PERFORM ${ref(params.schema)}."wake"(GREATEST(0, p_delay_ms));

                    RETURN JSONB_BUILD_OBJECT(
                        'result_code', ${value(MessageCreateResultCode.MESSAGE_CREATED)},
                        'id', v_message."id"
                    );
                END;
                $$ LANGUAGE plpgsql;
            `
        ]
    }
}
