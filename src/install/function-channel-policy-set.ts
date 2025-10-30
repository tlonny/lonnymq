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
                    p_id TEXT,
                    p_max_concurrency INTEGER,
                    p_max_size INTEGER,
                    p_release_interval_ms INTEGER
                ) RETURNS VOID AS $$
                BEGIN
                    INSERT INTO ${ref(params.schema)}."channel_policy" (
                        "id",
                        "max_concurrency",
                        "max_size",
                        "release_interval_ms"
                    ) VALUES (
                        p_id,
                        p_max_concurrency,
                        p_max_size,
                        p_release_interval_ms
                    ) ON CONFLICT ("id") DO UPDATE SET
                        "max_concurrency" = EXCLUDED."max_concurrency",
                        "max_size" = EXCLUDED."max_size",
                        "release_interval_ms" = EXCLUDED."release_interval_ms";

                    UPDATE ${ref(params.schema)}."channel_state" SET
                        "max_concurrency" = p_max_concurrency,
                        "max_size" = p_max_size,
                        "release_interval_ms" = p_release_interval_ms
                    WHERE "id" = p_id;
                END;
                $$ LANGUAGE plpgsql;
            `
        ]
    }
}
