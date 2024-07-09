import { DataSourceOptions } from 'typeorm'
import { Pool } from 'pg'
import { Embeddings } from '@langchain/core/embeddings'
import { Document } from '@langchain/core/documents'
import { TypeORMVectorStore, TypeORMVectorStoreDocument } from '@langchain/community/vectorstores/typeorm'
import { ICommonObject, INode, INodeData, INodeOutputsValue, INodeParams } from '../../../src/Interface'
import { getBaseClasses, getCredentialData, getCredentialParam } from '../../../src/utils'

class Postgres_Existing_VectorStores implements INode {
    label: string
    name: string
    version: number
    description: string
    type: string
    icon: string
    category: string
    badge: string
    baseClasses: string[]
    inputs: INodeParams[]
    credential: INodeParams
    outputs: INodeOutputsValue[]

    constructor() {
        this.label = 'Postgres Load Existing Index'
        this.name = 'postgresExistingIndex'
        this.version = 2.0
        this.type = 'Postgres'
        this.icon = 'postgres.svg'
        this.category = 'Vector Stores'
        this.description = 'Load existing index from Postgres using pgvector (i.e: Document has been upserted)'
        this.baseClasses = [this.type, 'VectorStoreRetriever', 'BaseRetriever']
        this.badge = 'DEPRECATING'
        this.credential = {
            label: 'Connect Credential',
            name: 'credential',
            type: 'credential',
            credentialNames: ['PostgresApi']
        }
        this.inputs = [
            {
                label: 'Embeddings',
                name: 'embeddings',
                type: 'Embeddings'
            },
            {
                label: 'Host',
                name: 'host',
                type: 'string'
            },
            {
                label: 'Database',
                name: 'database',
                type: 'string'
            },
            {
                label: 'SSL Connection',
                name: 'sslConnection',
                type: 'boolean',
                default: false,
                optional: false
            },
            {
                label: 'Port',
                name: 'port',
                type: 'number',
                placeholder: '6432',
                optional: true
            },
            {
                label: 'Table Name',
                name: 'tableName',
                type: 'string',
                placeholder: 'documents',
                additionalParams: true,
                optional: true
            },
            {
                label: 'Additional Configuration',
                name: 'additionalConfig',
                type: 'json',
                additionalParams: true,
                optional: true
            },
            {
                label: 'WHERE Clause',
                name: 'filter',
                type: 'string',
                additionalParams: true,
                optional: true
            },
            {
                label: 'Top K',
                name: 'topK',
                description: 'Number of top results to fetch. Default to 4',
                placeholder: '4',
                type: 'number',
                additionalParams: true,
                optional: true
            }
        ]
        this.outputs = [
            {
                label: 'Postgres Retriever',
                name: 'retriever',
                baseClasses: this.baseClasses
            },
            {
                label: 'Postgres Vector Store',
                name: 'vectorStore',
                baseClasses: [this.type, ...getBaseClasses(TypeORMVectorStore)]
            }
        ]
    }

    async init(nodeData: INodeData, _: string, options: ICommonObject): Promise<any> {
        const credentialData = await getCredentialData(nodeData.credential ?? '', options)
        const user = getCredentialParam('user', credentialData, nodeData)
        const password = getCredentialParam('password', credentialData, nodeData)
        const _tableName = nodeData.inputs?.tableName as string
        const tableName = _tableName ? _tableName : 'documents'
        const embeddings = nodeData.inputs?.embeddings as Embeddings
        const additionalConfig = nodeData.inputs?.additionalConfig as string
        const output = nodeData.outputs?.output as string
        const topK = nodeData.inputs?.topK as string
        const k = topK ? parseFloat(topK) : 4
        const input_filter = nodeData.inputs?.filter as string

        let additionalConfiguration = {}
        if (additionalConfig) {
            try {
                additionalConfiguration = typeof additionalConfig === 'object' ? additionalConfig : JSON.parse(additionalConfig)
            } catch (exception) {
                throw new Error('Invalid JSON in the Additional Configuration: ' + exception)
            }
        }
        // Parse filter field to json to use it for metadata filtering
       /* let filter = {}
        if (filter_field) {
            try {
                filter = typeof filter_field === 'object' ? filter_field : JSON.parse(filter_field)
            } catch (exception) {
                throw new Error('Invalid JSON in the Additional Configuration: ' + exception)
            }
        }*/


        const postgresConnectionOptions = {
            ...additionalConfiguration,
            type: 'postgres',
            host: nodeData.inputs?.host as string,
            port: nodeData.inputs?.port as number,
            username: user,
            password: password,
            database: nodeData.inputs?.database as string,
            ssl: {
                // Set sslmode to require for a secure connection
                rejectUnauthorized: false,
                sslmode: 'no-verify',
                },
        }

        const args = {
            postgresConnectionOptions: postgresConnectionOptions as DataSourceOptions,
            tableName: tableName
        }

        const vectorStore = await TypeORMVectorStore.fromDataSource(embeddings, args)

        // Rewrite the method to use pg pool connection instead of the default connection
        /* Otherwise a connection error is displayed when the chain tries to execute the function
            [chain/start] [1:chain:ConversationalRetrievalQAChain] Entering Chain run with input: { "question": "what the document is about", "chat_history": [] }
            [retriever/start] [1:chain:ConversationalRetrievalQAChain > 2:retriever:VectorStoreRetriever] Entering Retriever run with input: { "query": "what the document is about" }
            [ERROR]: uncaughtException:  Illegal invocation TypeError: Illegal invocation at Socket.ref (node:net:1524:18) at Connection.ref (.../node_modules/pg/lib/connection.js:183:17) at Client.ref (.../node_modules/pg/lib/client.js:591:21) at BoundPool._pulseQueue (/node_modules/pg-pool/index.js:148:28) at .../node_modules/pg-pool/index.js:184:37 at process.processTicksAndRejections (node:internal/process/task_queues:77:11)
        */
        // Original definition: vectorStore.similaritySearchVectorWithScore = async (query: number[], k: number, filter?: any) => { 
            // Original where clause: WHERE metadata @> $2
            // Overriding function definition to allow passing were clause as filter.
            vectorStore.similaritySearchVectorWithScore = async (query: number[], k: number, filter?: any) => {
            const embeddingString = `[${query.join(',')}]`
            const _filter = filter ?? '{}'
            const query_filter = input_filter ? ('AND ' + input_filter) : ''
            
            const queryString = `
                    SELECT *, embedding <=> $1 as "_distance"
                    FROM ${tableName}
                    WHERE metadata @> $2
                    ${query_filter}
                    ORDER BY "_distance" ASC
                    LIMIT $3;`

            const poolOptions = {
                host: postgresConnectionOptions.host,
                port: postgresConnectionOptions.port,
                user: postgresConnectionOptions.username,
                password: postgresConnectionOptions.password,
                database: postgresConnectionOptions.database,
                ssl: {
                    // Set sslmode to require for a secure connection
                    rejectUnauthorized: false,
                    sslmode: 'no-verify',
                    },
            }
            const pool = new Pool(poolOptions)
            const conn = await pool.connect()
            
            const documents = await conn.query(queryString, [embeddingString, _filter, k])

            conn.release()

            const results = [] as [TypeORMVectorStoreDocument, number][]
            for (const doc of documents.rows) {
                if (doc._distance != null && doc.pageContent != null) {
                    const document = new Document(doc) as TypeORMVectorStoreDocument
                    document.id = doc.id
                    results.push([document, doc._distance])
                }
            }

            return results
        }

        if (output === 'retriever') {
            // Add filter to the retriever for metadata filtering: const retriever = vectorStore.asRetriever(k,filter)
            const retriever = vectorStore.asRetriever(k)
            return retriever
        } else if (output === 'vectorStore') {
            ;(vectorStore as any).k = k
            // Add ;(vectorStore as any).filter = filter for metadata filtering
            return vectorStore
        }
        return vectorStore
    }
}

module.exports = { nodeClass: Postgres_Existing_VectorStores }