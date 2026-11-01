import { css, node } from "@lonnycorp/htmlforge"
import { COLOR } from "@www/constant.ts"

type Link = {
    key: MainNavKey
    label: string
    href: string
}

export const MAIN_NAV_KEY = {
    Home: "home",
    Diy: "diy",
    Source: "source",
} as const

export type MainNavKey = typeof MAIN_NAV_KEY[keyof typeof MAIN_NAV_KEY]

const LINKS: Link[] = [
    { key: MAIN_NAV_KEY.Home, label: "home", href: "/" },
    { key: MAIN_NAV_KEY.Diy, label: "diy", href: "/diy.html" },
    { key: MAIN_NAV_KEY.Source, label: "source", href: "https://github.com/tlonny/lonnymq" },
]

type NavOptions = {
    selectedKey?: MainNavKey
    links?: Link[]
}

export class Nav implements node.Buildable {
    private readonly selectedKey?: MainNavKey

    private readonly root = new node.Element("nav")
        .css(cls => css.css`
            .${cls} {
                display: flex;
                flex-wrap: wrap;
                gap: 8px;
                justify-content: center;
                line-height: 1.5;
                margin: 34px 0 0;
            }
            @media (max-width: 640px) {
                .${cls} { gap: 6px; margin: 12px 0 0; }
            }
        `)

    constructor(options: NavOptions = {}) {
        const links = options.links ?? LINKS
        this.selectedKey = options.selectedKey

        this.root
            .child(
                new node.Element("span")
                    .css(cls => css.css`
                        .${cls} {
                            color: ${COLOR.Brand};
                            display: none;
                            font-weight: 700;
                        }
                        @media (max-width: 640px) {
                            .${cls} { display: inline; }
                        }
                    `)
                    .child(new node.Text("LonnyMQ"))
            )
            .child(this.createSeparator({ mobileOnly: true }))

        for (let index = 0; index < links.length; index += 1) {
            const link = links[index]

            if (index > 0) {
                this.root.child(this.createSeparator())
            }

            this.root.child(this.createLink(link))
        }
    }

    build() {
        return this.root.build()
    }

    private createLink(link: Link) {
        if (link.key === this.selectedKey) {
            return new node.Element("span")
                .css(cls => css.css`
                    .${cls} {
                        color: ${COLOR.Inactive};
                        font-weight: 700;
                        padding: 0 2px;
                    }
                `)
                .child(new node.Text(link.label))
        }

        return new node.Element("a")
            .attribute("href", link.href)
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
            .child(new node.Text(link.label))
    }

    private createSeparator(options?: { mobileOnly?: boolean }) {
        const separator = new node.Element("span")
            .css(cls => css.css`
                .${cls} { color: ${COLOR.Link}; }
            `)
            .child(new node.Text("|"))

        if (options?.mobileOnly) {
            separator.css(cls => css.css`
                .${cls} { display: none; }
                @media (max-width: 640px) {
                    .${cls} { display: inline; }
                }
            `)
        }

        return separator
    }
}
