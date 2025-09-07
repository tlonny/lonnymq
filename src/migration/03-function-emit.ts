import { pathNormalize } from "@src/core/path"
import { value, sql, ref } from "@src/core/sql"

export const migrationFunctionEmit = {
    name: pathNormalize(__filename),
    sql: (params : {
        schema: string,
        eventChannel: string | null,
    }) => {
        const notifyFragment = params.eventChannel
            ? sql`PERFORM PG_NOTIFY(${value(params.eventChannel)}, p_data);`
            : sql``

        return [
            sql`
                CREATE FUNCTION ${ref(params.schema)}."emit" (
                    p_data TEXT
                ) RETURNS VOID AS $$
                BEGIN
                    ${notifyFragment}
                END;
                $$ LANGUAGE plpgsql;
            `
        ]
    }
}
