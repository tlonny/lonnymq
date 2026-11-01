import { css, node } from "@lonnycorp/htmlforge"
import { COLOR } from "@www/constant.ts"

export class Footer implements node.Buildable {
    private readonly root = new node.Element("footer")
        .css(cls => css.css`
            .${cls} {
                background: #000000;
                box-sizing: border-box;
                color: ${COLOR.Link};
                font-size: 0.72rem;
                line-height: 1.5;
                margin: 0;
                padding: 8px 24px;
                text-align: center;
                width: 100%;
            }
        `)
        .child(new node.Text("Created by the "))
        .child(
            new node.Element("a")
                .attribute("href", "https://www.lonnycorp.com")
                .css(cls => css.css`
                    .${cls} {
                        color: ${COLOR.Link};
                        font-weight: 700;
                        padding: 0 2px;
                        text-decoration: underline;
                    }
                    .${cls}:hover {
                        background: ${COLOR.LinkHoverBackground};
                        color: ${COLOR.LinkHoverText};
                    }
                `)
                .child(new node.Text("Lonny Corporation"))
        )

    build() {
        return this.root.build()
    }
}
