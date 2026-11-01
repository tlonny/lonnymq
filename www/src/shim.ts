import { node } from "@lonnycorp/htmlforge"
import { COLOR } from "@www/constant.ts"

const INK = COLOR.Brand

export const styleBody = (body: node.Element) => {
    body
        .style("background", "#000000")
        .style("box-sizing", "border-box")
        .style("font-size", "0.8rem")
        .style("color", INK)
        .style("display", "flex")
        .style("flex-direction", "column")
        .style("font-family", "monospace")
        .style("align-items", "center")
        .style("justify-content", "flex-start")
        .style("margin", "0")
        .style("min-height", "100vh")
        .style("padding", "0")
}
