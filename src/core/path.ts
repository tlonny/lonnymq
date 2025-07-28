import { dirname } from "path"

const ROOT_DIRECTORY = dirname(dirname(__filename))
const REGEXP = new RegExp(`^${ROOT_DIRECTORY}/`)

export const pathNormalize = (path : string): string => {
    return path.replace(REGEXP, "")
}
