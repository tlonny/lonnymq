import { pathNormalize } from "@src/core/path"
import { ref, sql } from "@src/core/sql"

export const migrationFunctionChannelPolicySet = {
    name: pathNormalize(__filename),
    sql: (params : {
        schema: string,
    }) => {
        return [
            sql`
                CREATE FUNCTION ${ref(params.schema)}."channel_policy_set" (
                    p_name TEXT,
                    p_max_size BIGINT,
                    p_max_concurrency BIGINT
                ) RETURNS VOID AS $$
                BEGIN
                    INSERT INTO ${ref(params.schema)}."channel_policy" (
                        "name",
                        "max_size",
                        "max_concurrency"
                    ) VALUES (
                        p_name,
                        p_max_size,
                        p_max_concurrency
                    ) ON CONFLICT ("name") DO UPDATE SET
                        "max_size" = EXCLUDED."max_size",
                        "max_concurrency" = EXCLUDED."max_concurrency";

                    UPDATE ${ref(params.schema)}."channel_state" SET
                        "max_size" = p_max_size,
                        "max_concurrency" = p_max_concurrency
                    WHERE "name" = p_name;

                    PERFORM ${ref(params.schema)}."wake"(0);
                END;
                $$ LANGUAGE plpgsql;
            `
        ]
    }
}
