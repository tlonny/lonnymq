import { pathNormalize } from "@src/core/path"
import { ref, sql } from "@src/core/sql"

export const installFunctionChannelPolicySet = {
    name: pathNormalize(__filename),
    sql: (params : {
        schema: string,
    }) => {
        return [
            sql`
                CREATE FUNCTION ${ref(params.schema)}."channel_policy_set" (
                    p_name TEXT,
                    p_max_concurrency INTEGER,
                    p_release_interval_ms INTEGER
                ) RETURNS VOID AS $$
                BEGIN
                    INSERT INTO ${ref(params.schema)}."channel_policy" (
                        "name",
                        "max_concurrency",
                        "release_interval_ms"
                    ) VALUES (
                        p_name,
                        p_max_concurrency,
                        p_release_interval_ms
                    ) ON CONFLICT ("name") DO UPDATE SET
                        "max_concurrency" = EXCLUDED."max_concurrency",
                        "release_interval_ms" = EXCLUDED."release_interval_ms";

                    UPDATE ${ref(params.schema)}."channel_state" SET
                        "max_concurrency" = p_max_concurrency,
                        "release_interval_ms" = p_release_interval_ms
                    WHERE "name" = p_name;
                END;
                $$ LANGUAGE plpgsql;
            `
        ]
    }
}
