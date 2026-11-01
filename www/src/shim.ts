import { css, node } from "@lonnycorp/htmlforge"
import { COLOR } from "@www/constant.ts"

const INK = COLOR.Brand

export const styleBody = (body: node.Element) => {
    body.css(cls => css.css`
        html { background: #000000; }
        .${cls} {
            background: #000000;
            box-sizing: border-box;
            font-size: 0.8rem;
            color: ${INK};
            display: flex;
            flex-direction: column;
            font-family: monospace;
            align-items: center;
            justify-content: flex-start;
            margin: 0;
            min-height: 100vh;
            padding: 0;
        }
    `)
}
