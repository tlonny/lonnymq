import { pathNormalize } from "@src/core/path"
import { ref, sql } from "@src/core/sql"

export const migrationTableChannelState = {
    name: pathNormalize(__filename),
    sql: (params : {
        schema: string,
    }) => {
        return [
            sql`
                CREATE TABLE ${ref(params.schema)}."channel_state" (
                    "id" UUID NOT NULL DEFAULT GEN_RANDOM_UUID(),
                    "name" TEXT NOT NULL,
                    "max_size" INTEGER,
                    "max_concurrency" INTEGER,
                    "release_interval_ms" INTEGER,
                    "current_size" INTEGER NOT NULL,
                    "current_concurrency" INTEGER NOT NULL,
                    "message_next_id" UUID,
                    "message_next_dequeue_after" TIMESTAMP,
                    "message_next_seq_no" BIGINT,
                    PRIMARY KEY ("id")
                );
            `,
            sql`
                CREATE UNIQUE INDEX "channel_state_name_ux"
                ON ${ref(params.schema)}."channel_state" ("name");
            `,
            sql`
                CREATE INDEX "channel_state_dequeue_ix"
                ON ${ref(params.schema)}."channel_state" (
                    "message_next_dequeue_after" ASC
                ) WHERE "message_next_id" IS NOT NULL
                AND ("max_concurrency" IS NULL OR "current_concurrency" < "max_concurrency");
            `
        ]
    }
}
