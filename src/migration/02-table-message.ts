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
                    "id" UUID NOT NULL,
                    "channel_name" TEXT NOT NULL,
                    "seq_no" BIGSERIAL NOT NULL,
                    "name" TEXT,
                    "content" BYTEA NOT NULL,
                    "state" BYTEA,
                    "lock_ms" INTEGER NOT NULL,
                    "is_locked" BOOLEAN NOT NULL DEFAULT FALSE,
                    "num_attempts" INTEGER NOT NULL DEFAULT 0,
                    "dequeue_at" TIMESTAMP NOT NULL,
                    "unlock_at" TIMESTAMP,
                    "created_at" TIMESTAMP NOT NULL,
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
                    "dequeue_at" ASC,
                    "seq_no" ASC
                ) WHERE NOT "is_locked";
            `,

            sql`
                CREATE INDEX "message_locked_dequeue_ix"
                ON ${ref(params.schema)}."message" (
                    "unlock_at" ASC
                ) WHERE "is_locked";
            `
        ]
    }
}
