import type { Code as MdastCode } from "mdast"
import { css, node } from "@lonnycorp/htmlforge"

export class Code implements node.Buildable {
    private readonly root: node.Element

    constructor(code: MdastCode) {
        this.root = new node.Element("pre")
            .css(cls => css.css`
                .${cls} {
                    border: 1px solid #2F2F2F;
                    box-shadow: 4px 4px 0 #050505;
                    box-sizing: border-box;
                    font-size: 13px;
                    line-height: 1.45;
                    margin: 0 0 18px;
                    overflow-x: auto;
                    width: 100%;
                }
            `)
            .child(
                new node.Element("code")
                    .attribute("class", `language-${code.lang ?? "text"}`)
                    .child(new node.Text(code.value))
            )
    }

    build() {
        return this.root.build()
    }
}
