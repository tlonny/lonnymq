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
                BEGIN
                    DELETE FROM ${ref(params.schema)}."channel_policy" 
                    WHERE "name" = p_name;

                    UPDATE ${ref(params.schema)}."channel_state" SET
                        "max_size" = NULL,
                        "max_concurrency" = NULL
                    WHERE "name" = p_name;

                    PERFORM ${ref(params.schema)}."wake"(0);
                END;
                $$ LANGUAGE plpgsql;
            `
        ]
    }
}
