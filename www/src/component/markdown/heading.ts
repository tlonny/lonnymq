import { css, node } from "@lonnycorp/htmlforge"
import { COLOR } from "@www/constant.ts"

export enum Kind {
    H1 = "h1",
    H2 = "h2",
    H3 = "h3",
}

const COLOR_BY_KIND: Record<Kind, string> = {
    [Kind.H1]: COLOR.Brand,
    [Kind.H2]: "#C2FF2B",
    [Kind.H3]: "#CEFF55",
}

export class Heading implements node.Buildable {
    private readonly root: node.Element

    constructor(kind: Kind) {
        const style = getStyle(kind)

        this.root = new node.Element(kind)
            .css(cls => css.css`
                .${cls} {
                    color: ${COLOR_BY_KIND[kind]};
                    font-size: ${style.fontSize};
                    font-weight: 700;
                    line-height: 1.2;
                    margin: ${style.margin};
                    text-transform: uppercase;
                }
            `)
    }

    child(child: node.Buildable | string) {
        this.root.child(typeof child === "string" ? new node.Text(child) : child)
        return this
    }

    build() {
        return this.root.build()
    }
}

export const kindFromDepth = (depth: number) => {
    if (depth === 1) {
        return Kind.H1
    }

    if (depth === 2) {
        return Kind.H2
    }

    if (depth === 3) {
        return Kind.H3
    }

    throw new Error(`Markdown heading depth is not supported: ${depth}`)
}

const getStyle = (kind: Kind) => {
    if (kind === Kind.H1) {
        return {
            fontSize: "1.45rem",
            margin: "0 0 18px",
        }
    }

    if (kind === Kind.H2) {
        return {
            fontSize: "1.2rem",
            margin: "34px 0 16px",
        }
    }

    return {
        fontSize: "1rem",
        margin: "28px 0 14px",
    }
}
