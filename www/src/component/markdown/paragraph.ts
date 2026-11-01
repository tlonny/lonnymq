import type { Paragraph as MdastParagraph } from "mdast"
import { css, node } from "@lonnycorp/htmlforge"
import { Phrasing } from "@www/component/markdown/phrasing.ts"

export class Paragraph implements node.Buildable {
    private readonly root: node.Element

    constructor(paragraph: MdastParagraph) {
        this.root = new node.Element("p")
            .css(cls => css.css`
                .${cls} { margin: 0 0 18px; }
            `)
            .child(new Phrasing(paragraph.children))
    }

    build() {
        return this.root.build()
    }
}
