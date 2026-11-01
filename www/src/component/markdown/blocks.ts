import type { BlockContent, RootContent } from "mdast"
import { node } from "@lonnycorp/htmlforge"
import { Block } from "@www/component/markdown/block.ts"

export class Blocks implements node.Buildable {
    private readonly root = new node.Fragment()

    constructor(blocks: Array<BlockContent | RootContent>) {
        for (const block of blocks) {
            this.root.child(new Block(block))
        }
    }

    build() {
        return this.root.build()
    }
}
