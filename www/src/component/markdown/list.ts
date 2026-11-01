import type { List as MdastList } from "mdast"
import { css, node } from "@lonnycorp/htmlforge"
import { ListItem } from "@www/component/markdown/list-item.ts"

export class List implements node.Buildable {
    private readonly root: node.Element

    constructor(list: MdastList) {
        this.root = new node.Element(list.ordered ? "ol" : "ul")
            .css(cls => css.css`
                .${cls} {
                    margin: ${list.ordered ? "0 0 18px" : "0"};
                    padding: 0 0 0 24px;
                }
            `)

        for (const item of list.children) {
            this.root.child(
                new node.Element("li")
                    .css(cls => css.css`
                        .${cls} { margin: 6px 0; }
                    `)
                    .child(new ListItem(item))
            )
        }
    }

    build() {
        return this.root.build()
    }
}
