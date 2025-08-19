import { WAKE_CHANNEL } from "@src/core/constant"
import { pathNormalize } from "@src/core/path"
import { value, sql, ref } from "@src/core/sql"

export const migrationFunctionWake = {
    name: pathNormalize(__filename),
    sql: (params : {
        schema: string,
        useWake: boolean
    }) => {
        const channel = WAKE_CHANNEL.toString(params.schema)
        return [
            sql`
                CREATE FUNCTION ${ref(params.schema)}."wake" (
                    p_delay_ms INTEGER
                ) RETURNS VOID AS $$
                BEGIN
                    IF ${value(params.useWake)} THEN
                        PERFORM PG_NOTIFY(${value(channel)}, p_delay_ms::TEXT);
                    END IF;
                END;
                $$ LANGUAGE plpgsql;
            `
        ]
    }
}
