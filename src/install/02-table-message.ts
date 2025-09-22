import { pathNormalize } from "@src/core/path"
import { ref, sql } from "@src/core/sql"

export const installTableMessage = {
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
                    "content" BYTEA NOT NULL,
                    "state" BYTEA,
                    "lock_ms" BIGINT NOT NULL,
                    "is_locked" BOOLEAN NOT NULL,
                    "num_attempts" BIGINT NOT NULL,
                    "dequeue_at" TIMESTAMP NOT NULL,
                    "unlock_at" TIMESTAMP,
                    "created_at" TIMESTAMP NOT NULL,
                    PRIMARY KEY ("id")
                );
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
