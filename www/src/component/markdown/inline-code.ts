import type { InlineCode as MdastInlineCode } from "mdast"
import { css, node } from "@lonnycorp/htmlforge"
import { COLOR } from "@www/constant.ts"

export class InlineCode implements node.Buildable {
    private readonly root: node.Element

    constructor(code: MdastInlineCode) {
        this.root = new node.Element("code")
            .css(cls => css.css`
                .${cls} {
                    background: #0B0B0B;
                    color: ${COLOR.Brand};
                    padding: 0 3px;
                }
            `)
            .child(new node.Text(code.value))
    }

    build() {
        return this.root.build()
    }
}
