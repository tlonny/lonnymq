import { pathNormalize } from "@src/core/path"
import { ref, sql } from "@src/core/sql"

export const migrationFunctionChannelPolicyClear = {
    name: pathNormalize(__filename),
    sql: (params : {
        schema: string,
    }) => {
        return [
            sql`
                CREATE FUNCTION ${ref(params.schema)}."channel_policy_clear" (
                    p_name TEXT
                ) RETURNS VOID AS $$
                DECLARE
                    v_channel_state RECORD;
                BEGIN
                    DELETE FROM ${ref(params.schema)}."channel_policy" 
                    WHERE "name" = p_name;

                    SELECT
                        "channel_state"."id",
                        "channel_state"."current_size"
                    FROM ${ref(params.schema)}."channel_state"
                    WHERE "name" = p_name
                    FOR UPDATE
                    INTO v_channel_state;

                    IF v_channel_state."current_size" = 0 THEN
                        DELETE FROM ${ref(params.schema)}."channel_state"
                        WHERE "id" = v_channel_state."id";
                    ELSE
                        UPDATE ${ref(params.schema)}."channel_state" SET
                            "max_size" = NULL,
                            "max_concurrency" = NULL,
                            "release_interval_ms" = NULL
                        WHERE "name" = p_name;
                    END IF;

                    PERFORM ${ref(params.schema)}."wake"(0);
                END;
                $$ LANGUAGE plpgsql;
            `
        ]
    }
}
