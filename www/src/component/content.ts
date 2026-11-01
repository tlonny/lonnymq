import { css, node } from "@lonnycorp/htmlforge"

export class Content implements node.Buildable {
    private readonly root = new node.Element("section")
        .css(cls => css.css`
            .${cls} {
                background: #181818;
                box-shadow: 6px 6px 0 #050505;
                box-sizing: border-box;
                color: #FFFFFF;
                line-height: 1.55;
                margin: 26px auto 0;
                max-width: 720px;
                padding: 24px;
                width: 100%;
            }
        `)

    child(child: node.Buildable) {
        this.root.child(child)
        return this
    }

    build() {
        return this.root.build()
    }
}
