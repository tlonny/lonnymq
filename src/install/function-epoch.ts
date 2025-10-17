import { pathNormalize } from "@src/core/path"
import { ref, sql } from "@src/core/sql"

export const installFunctionEpoch = {
    name: pathNormalize(__filename),
    sql: (params : {
        schema: string,
        eventChannel: string | null,
    }) => {
        return [
            sql`
                CREATE FUNCTION ${ref(params.schema)}."epoch" () 
                RETURNS BIGINT AS $$
                DECLARE
                    v_now TIMESTAMPTZ;
                BEGIN
                    v_now := NOW();
                    RETURN 
                        EXTRACT(EPOCH FROM v_now)::BIGINT * 1_000 +
                        EXTRACT(MILLISECOND FROM v_now)::BIGINT;
                END;
                $$ LANGUAGE plpgsql;
            `
        ]
    }
}
