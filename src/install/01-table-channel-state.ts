import { pathNormalize } from "@src/core/path"
import { ref, sql } from "@src/core/sql"

export const installTableChannelState = {
    name: pathNormalize(__filename),
    sql: (params : {
        schema: string,
    }) => {
        const nameIndex = [params.schema, "channel_state_name_ux"].join("_")
        const dequeueIndex = [params.schema, "channel_state_dequeue_ix"].join("_")

        return [
            sql`
                CREATE TABLE ${ref(params.schema)}."channel_state" (
                    "id" UUID NOT NULL DEFAULT GEN_RANDOM_UUID(),
                    "name" TEXT NOT NULL,
                    "max_concurrency" INTEGER,
                    "release_interval_ms" INTEGER,
                    "current_size" INTEGER NOT NULL,
                    "current_concurrency" INTEGER NOT NULL,
                    "message_id" UUID,
                    "message_seq_no" BIGINT,
                    "message_dequeue_at" TIMESTAMP,
                    "active_prev_at" TIMESTAMP NOT NULL,
                    "active_next_at" TIMESTAMP NULL,
                    "created_at" TIMESTAMP NOT NULL,
                    PRIMARY KEY ("id")
                );
            `,
            sql`
                CREATE UNIQUE INDEX ${ref(nameIndex)}
                ON ${ref(params.schema)}."channel_state" ("name");
            `,
            sql`
                CREATE INDEX ${ref(dequeueIndex)}
                ON ${ref(params.schema)}."channel_state" (
                    "active_next_at" ASC
                ) WHERE "message_id" IS NOT NULL
                AND ("max_concurrency" IS NULL OR "current_concurrency" < "max_concurrency");
            `
        ]
    }
}
