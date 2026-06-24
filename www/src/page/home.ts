import { readFileSync } from "fs"
import { join } from "path"
import { node } from "@lonnycorp/htmlforge"
import { Content } from "@www/component/content.ts"
import { Main } from "@www/component/main.ts"
import { Markdown } from "@www/component/markdown/index.ts"
import { MAIN_NAV_KEY } from "@www/component/nav.ts"
import { CONTENT_DIRECTORY } from "@www/constant.ts"

const CONTENT_PATH = join(CONTENT_DIRECTORY, "home.md")

export class HomePage implements node.Buildable {
    private readonly root = new Main({ selectedNavKey: MAIN_NAV_KEY.Home })
        .child(
            new Content()
                .child(new Markdown(readFileSync(CONTENT_PATH, "utf8")))
        )

    build() {
        return this.root.build()
    }
}
