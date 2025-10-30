import { pathNormalize } from "@src/core/path"
import { ref, sql } from "@src/core/sql"

export const installTableChannelPolicy = {
    name: pathNormalize(__filename),
    sql: (params : {
        schema: string,
    }) => {
        return [
            sql`
                CREATE TABLE ${ref(params.schema)}."channel_policy" (
                    "id" TEXT NOT NULL,
                    "max_concurrency" INTEGER,
                    "max_size" INTEGER,
                    "release_interval_ms" INTEGER,
                    PRIMARY KEY ("id")
                );
            `,
        ]
    }
}
