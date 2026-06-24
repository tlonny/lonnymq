import type { Strong as MdastStrong } from "mdast"
import { node } from "@lonnycorp/htmlforge"
import { Phrasing } from "@www/component/markdown/phrasing.ts"
import { COLOR } from "@www/constant.ts"

export class Strong implements node.Buildable {
    private readonly root: node.Element

    constructor(strong: MdastStrong) {
        this.root = new node.Element("span")
            .style("color", COLOR.Brand)
            .style("font-weight", "700")
            .child(new Phrasing(strong.children))
    }

    build() {
        return this.root.build()
    }
}
