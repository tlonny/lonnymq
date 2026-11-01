import type { ListItem as MdastListItem } from "mdast"
import { node } from "@lonnycorp/htmlforge"
import { Phrasing } from "@www/component/markdown/phrasing.ts"

export class ListItem implements node.Buildable {
    private readonly root: node.Buildable

    constructor(item: MdastListItem) {
        if (item.children.length === 1 && item.children[0]?.type === "paragraph") {
            this.root = new Phrasing(item.children[0].children)
            return
        }

        throw new Error("Markdown list items must contain a single paragraph")
    }

    build() {
        return this.root.build()
    }
}
