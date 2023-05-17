import * as lodash from "lodash"
import { DatabaseInterface, Fookie } from "fookie-types"

import { v4 } from "uuid"
import * as dynamoose from "dynamoose"

export function initDynamoDB(fookie: Fookie): DatabaseInterface {
    const typeMap = [
        {
            from: fookie.Type.Text,
            to: String,
        },
        {
            from: fookie.Type.Integer,
            to: Number,
        },

        {
            from: fookie.Type.Float,
            to: Number,
        },
        {
            from: fookie.Type.Boolean,
            to: Boolean,
        },
        {
            from: fookie.Type.Plain,
            to: Object,
        },

        {
            from: fookie.Type.FloatArray,
            to: Array,
        },
        {
            from: fookie.Type.StringArray,
            to: String,
        },

        {
            from: fookie.Type.IntegerArray,
            to: Array,
        },
    ]

    const ddb = new dynamoose.aws.ddb.DynamoDB({
        credentials: {
            accessKeyId: process.env.AWS_ACCESS_KEY,
            secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
        },
        region: process.env.AWS_REGION,
    })
    dynamoose.aws.ddb.set(ddb)

    return fookie.Core.database({
        pk_type: fookie.Type.Text,
        pk: "id",
        connect: async function () {},
        disconnect: async function () {},
        modify: async function (model) {
            const defaultSchema = {
                id: {
                    type: String,
                    hashKey: true,
                },
                ts: {
                    type: Number,
                    rangeKey: true,
                },
            }

            for (const field of lodash.keys(model.schema)) {
                const type = typeMap.find(function (tp) {
                    return tp.from === model.schema[field].type
                })

                console.log(type, field)
                if (!type) {
                    throw Error("invalid_dynamodb_type")
                }
                defaultSchema[field] = {
                    type: type.to,
                }
            }

            const schema = new dynamoose.Schema(defaultSchema)
            const MDL = dynamoose.model(model.name, schema)
            const Table = new dynamoose.Table(model.name, [MDL])
            model.methods = {}

            model.methods.read = async function (payload, state) {
                const filter = queryFixer(payload.query.filter, lodash)
                console.log(payload.query)

                const scan = MDL.scan(filter).attributes([...payload.query.attributes, "id", "ts"])
                const response = await scan.exec()
                payload.response.data = response
            }
            model.methods.create = async function (payload, state) {
                payload.body.id = v4()
                payload.body.ts = Date.now()
                const entity = new MDL(payload.body)
                const response = await entity.save()
                payload.response.data = response
            }
            model.methods.delete = async function (payload, state) {
                const filter = queryFixer(payload.query.filter, lodash)
                let scan = MDL.scan(filter).attributes(["id"])
                const list = await scan.exec()
                const response = await MDL.batchDelete(lodash.map(list, "id"))
                payload.response.data = response
            }
            model.methods.update = async function (payload, state) {
                const filter = queryFixer(payload.query.filter, lodash)
                const scan = MDL.scan(filter)
                let list = await scan.exec()

                const chunks = lodash.chunk(list, 25)

                for (const chunk of chunks) {
                    const updatePromises = chunk.map((item) => {
                        const updatedItem = lodash.merge(item, payload.body)
                        return MDL.update({ id: updatedItem.id, ts: updatedItem.ts }, updatedItem)
                    })
                    await Promise.all(updatePromises)
                }
                payload.response.data = true
            }

            model.methods.count = async function (payload, state) {
                const filter = queryFixer(payload.query.filter, lodash)
                let scan = MDL.scan(filter)
                const count = await scan.count().exec()

                payload.response.data = count
            }
        },
    })
}

const queryFixer = function (filter, lodash) {
    const filterMap = {
        eq: "eq",
        gt: "gt",
        lt: "lt",
        gte: "ge",
        lte: "le",
        not_eq: "ne",
        like: "contains",
        in: "in",
        not_in: "notIn",
    }
    const newQuery = {}
    for (const f in filter) {
        let field = filter[f]
        if (lodash.isObject(field)) {
            field = lodash.mapKeys(field, function (value, key) {
                return filterMap[key] ? filterMap[key] : key
            })
        }
        newQuery[f] = field
    }
    return newQuery
}
