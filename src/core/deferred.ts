export type DeferredGetResult<T> =
    | { resultType: "RESULT_NOT_SET" }
    | { resultType: "RESULT_SET"; value: T };

export class Deferred<T> {
    private value: T | null
    private isSet: boolean

    constructor() {
        this.isSet = false
        this.value = null
    }

    get() : DeferredGetResult<T> {
        return this.isSet
            ? { resultType: "RESULT_SET", value: this.value as T }
            : { resultType: "RESULT_NOT_SET" }
    }

    set(value: T): void {
        this.isSet = true
        this.value = value
    }
}
