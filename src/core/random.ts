import { randomBytes } from "crypto"

export const randomSlug = () => {
    return randomBytes(16).toString("base64url")
}
