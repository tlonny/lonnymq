import { css, node } from "@lonnycorp/htmlforge"
import { Logo } from "@www/component/logo.ts"
import { Nav, type MainNavKey } from "@www/component/nav.ts"

type MainOptions = {
    selectedNavKey?: MainNavKey
}

export class Main implements node.Buildable {
    private readonly root = new node.Element("main")
        .css(cls => css.css`
            .${cls} {
                box-sizing: border-box;
                flex: 1 0 auto;
                max-width: 980px;
                width: 100%;
            }
        `)

    constructor(options: MainOptions = {}) {
        this.root
            .child(new Logo())
            .child(new Nav({ selectedKey: options.selectedNavKey }))
    }

    child(child: node.Buildable) {
        this.root.child(child)
        return this
    }

    build() {
        return this.root.build()
    }
}
