import type { Paragraph as MdastParagraph } from "mdast"
import { node } from "@lonnycorp/htmlforge"
import { type BlockOptions } from "@www/component/markdown/blocks.ts"
import { Phrasing } from "@www/component/markdown/phrasing.ts"

export class Paragraph implements node.Buildable {
    private readonly root: node.Element

    constructor(paragraph: MdastParagraph, options: BlockOptions = {}) {
        this.root = new node.Element("p")
            .style("margin", options.compactParagraphs ? "0" : "0 0 18px")
            .child(new Phrasing(paragraph.children))
    }

    build() {
        return this.root.build()
    }
}
