import { pathNormalize } from "@src/core/path"
import { ref, sql } from "@src/core/sql"

export const installFunctionChannelPolicyClear = {
    name: pathNormalize(__filename),
    sql: (params : {
        schema: string,
    }) => {
        return [
            sql`
                CREATE FUNCTION ${ref(params.schema)}."channel_policy_clear" (
                    p_id TEXT
                ) RETURNS VOID AS $$
                DECLARE
                    v_channel_state RECORD;
                BEGIN
                    DELETE FROM ${ref(params.schema)}."channel_policy" 
                    WHERE "id" = p_id;

                    SELECT
                        "channel_state"."id",
                        "channel_state"."current_size"
                    FROM ${ref(params.schema)}."channel_state"
                    WHERE "id" = p_id
                    FOR UPDATE
                    INTO v_channel_state;

                    IF v_channel_state."current_size" = 0 THEN
                        DELETE FROM ${ref(params.schema)}."channel_state"
                        WHERE "id" = v_channel_state."id";
                    ELSE
                        UPDATE ${ref(params.schema)}."channel_state" SET
                            "max_concurrency" = NULL,
                            "release_interval_ms" = NULL
                        WHERE "id" = p_id;
                    END IF;
                END;
                $$ LANGUAGE plpgsql;
            `
        ]
    }
}
