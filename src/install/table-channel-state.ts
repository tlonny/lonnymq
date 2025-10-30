import { pathNormalize } from "@src/core/path"
import { ref, sql } from "@src/core/sql"

export const installTableChannelState = {
    name: pathNormalize(__filename),
    sql: (params : {
        schema: string,
    }) => {
        const dequeueIndex = [params.schema, "channel_state_dequeue_ix"].join("_")
        return [
            sql`
                CREATE TABLE ${ref(params.schema)}."channel_state" (
                    "id" TEXT NOT NULL,
                    "max_concurrency" INTEGER,
                    "max_size" INTEGER,
                    "release_interval_ms" INTEGER,
                    "current_size" INTEGER NOT NULL,
                    "current_concurrency" INTEGER NOT NULL,
                    "message_id" BIGINT,
                    "message_dequeue_at" BIGINT,
                    "dequeue_prev_at" BIGINT NOT NULL,
                    "dequeue_next_at" BIGINT NULL,
                    PRIMARY KEY ("id")
                );
            `,
            sql`
                CREATE INDEX ${ref(dequeueIndex)}
                ON ${ref(params.schema)}."channel_state" (
                    "dequeue_next_at" ASC
                ) WHERE "message_id" IS NOT NULL
                AND ("max_concurrency" IS NULL OR "current_concurrency" < "max_concurrency");
            `
        ]
    }
}
