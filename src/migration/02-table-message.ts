import { pathNormalize } from "@src/core/path"
import { ref, sql } from "@src/core/sql"

export const migrationTableMessage = {
    name: pathNormalize(__filename),
    sql: (params : {
        schema: string,
    }) => {
        return [
            sql`
                CREATE TABLE ${ref(params.schema)}."message" (
                    "id" UUID NOT NULL DEFAULT GEN_RANDOM_UUID(),
                    "channel_name" TEXT NOT NULL,
                    "dequeue_id" UUID,
                    "name" TEXT,
                    "content" TEXT NOT NULL,
                    "state" TEXT,
                    "lock_ms" BIGINT NOT NULL,
                    "is_locked" BOOLEAN NOT NULL DEFAULT FALSE,
                    "num_attempts" BIGINT NOT NULL DEFAULT 0,
                    "dequeue_after" TIMESTAMP NOT NULL,
                    "sweep_after" TIMESTAMP,
                    PRIMARY KEY ("id")
                );
            `,

            sql`
                CREATE UNIQUE INDEX "message_name_ux"
                ON ${ref(params.schema)}."message" (
                    "channel_name", 
                    "name"
                ) WHERE "num_attempts" = 0
            `,

            sql`
                CREATE INDEX "message_dequeue_ix"
                ON ${ref(params.schema)}."message" (
                    "channel_name",
                    "dequeue_after" ASC
                ) WHERE NOT "is_locked";
            `,

            sql`
                CREATE INDEX "message_locked_dequeue_ix"
                ON ${ref(params.schema)}."message" (
                    "dequeue_after" ASC
                ) WHERE "is_locked";
            `
        ]
    }
}
