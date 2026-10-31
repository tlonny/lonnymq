import * as util from "@src/util"

export const epoch = (params : {
    schema: string,
    eventChannel: string | null,
}) => {
    return [
        util.sql.fragment`
            CREATE FUNCTION ${util.sql.ref(params.schema)}."epoch" () 
            RETURNS BIGINT AS $$
            DECLARE
                v_now TIMESTAMPTZ;
            BEGIN
                v_now := NOW();
                RETURN FLOOR(EXTRACT(EPOCH FROM v_now) * 1_000)::BIGINT;
            END;
            $$ LANGUAGE plpgsql;
        `
    ]
}
