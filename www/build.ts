import { copyFileSync, mkdirSync, rmSync, writeFileSync } from "fs"
import { dirname, join } from "path"
import { Document, node } from "@lonnycorp/htmlforge"
import { Footer } from "@www/component/footer.ts"
import { Helmet } from "@www/component/helmet.ts"
import { IMAGES } from "@www/constant.ts"
import { styleBody } from "@www/shim.ts"
import { DIY_CHAPTERS, DiyPage } from "@www/page/diy.ts"
import { HomePage } from "@www/page/home.ts"

const ROOT_DIRECTORY = dirname(import.meta.dir)
const DIST_PATH = join(ROOT_DIRECTORY, "dist", "www")

const clean = () => {
    rmSync(DIST_PATH, { recursive: true, force: true })
    mkdirSync(DIST_PATH, { recursive: true })
}

const renderPage = (path: string, page: node.Buildable) => {
    const doc = new Document()

    doc.attribute("lang", "en")
    doc.head.child(new Helmet({ title: "LonnyMQ" }))
    styleBody(doc.body)
    doc.body.child(page)
    doc.body.child(new Footer())

    writeFileSync(join(DIST_PATH, path), doc.toString())
}

const buildImages = () => {
    for (const image of IMAGES) {
        const dstPath = join(DIST_PATH, image.publicPath.replace(/^\//, ""))

        mkdirSync(dirname(dstPath), { recursive: true })
        copyFileSync(image.srcPath, dstPath)
    }
}

const build = () => {
    clean()
    buildImages()
    renderPage("index.html", new HomePage())

    for (const chapter of DIY_CHAPTERS) {
        renderPage(chapter.outputPath, new DiyPage(chapter))
    }
}

build()
