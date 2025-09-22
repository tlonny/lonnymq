import { pathNormalize } from "@src/core/path"
import { ref, sql } from "@src/core/sql"

export const migrationTableChannelPolicy = {
    name: pathNormalize(__filename),
    sql: (params : {
        schema: string,
    }) => {
        return [
            sql`
                CREATE TABLE ${ref(params.schema)}."channel_policy" (
                    "id" UUID NOT NULL DEFAULT GEN_RANDOM_UUID(),
                    "name" TEXT NOT NULL,
                    "max_concurrency" INTEGER,
                    "release_interval_ms" INTEGER,
                    "created_at" TIMESTAMP NOT NULL,
                    PRIMARY KEY ("id")
                );
            `,

            sql`
                CREATE UNIQUE INDEX "channel_policy_name_ux"
                ON ${ref(params.schema)}."channel_policy" ("name");
            `,
        ]
    }
}
