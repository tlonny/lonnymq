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
                    "id" BIGSERIAL NOT NULL,
                    "channel_id" TEXT NOT NULL,
                    "content" BYTEA NOT NULL,
                    "state" BYTEA,
                    "is_locked" BOOLEAN NOT NULL,
                    "num_attempts" BIGINT NOT NULL,
                    "dequeue_at" BIGINT NOT NULL,
                    "unlock_at" BIGINT,
                    PRIMARY KEY ("id")
                );
            `,

            sql`
                CREATE INDEX "message_dequeue_ix"
                ON ${ref(params.schema)}."message" (
                    "channel_id",
                    "dequeue_at" ASC,
                    "id" ASC
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
