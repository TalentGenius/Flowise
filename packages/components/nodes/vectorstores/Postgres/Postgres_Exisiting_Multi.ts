import { ICommonObject, INode, INodeData, INodeOutputsValue, INodeParams } from '../../../src/Interface'
import { Embeddings } from 'langchain/embeddings/base'
import { Document } from 'langchain/document'
import { DataSourceOptions } from 'typeorm'
import { TypeORMVectorStore, TypeORMVectorStoreDocument } from 'langchain/vectorstores/typeorm'
import { getBaseClasses, getCredentialData, getCredentialParam } from '../../../src/utils'
import { Pool } from 'pg'
import { ConsoleCallbackHandler } from '../../../src/handler'

class Postgres_Multi_Existing_VectorStores implements INode {
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
        this.label = 'Postgres Multi Load Existing Index'
        this.name = 'postgresMultiExistingIndex'
        this.version = 1.0
        this.type = 'Postgres'
        this.icon = 'postgres.svg'
        this.category = 'Vector Stores'
        this.description = 'Load existing index from Postgres using pgvector (i.e: Document has been upserted)'
        this.baseClasses = [this.type, 'VectorStoreRetriever', 'BaseRetriever']
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
                label: 'Port',
                name: 'port',
                type: 'number',
                placeholder: '6432',
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
                label: 'FULL QUERY Clause',
                name: 'fullQuery',
                type: 'string',
                placeholder: ' ... LIMIT $1;',
                rows: 6,
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
        console.log("Entering init function of Postgres_Multi_Existing_VectorStores")
        const credentialData = await getCredentialData(nodeData.credential ?? '', options)
        const user = getCredentialParam('user', credentialData, nodeData)
        const password = getCredentialParam('password', credentialData, nodeData)
        const embeddings = nodeData.inputs?.embeddings as Embeddings
        const additionalConfig = nodeData.inputs?.additionalConfig as string
        const output = nodeData.outputs?.output as string
        const k = 4
        const full_query = nodeData.inputs?.fullQuery as string
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
                sslmode: 'no-verify'
            }
        }

        const args = {
            postgresConnectionOptions: postgresConnectionOptions as DataSourceOptions
        }

        const vectorStore = await TypeORMVectorStore.fromDataSource(embeddings, args) as any

        // query: { [key: string]: number[] } its a dictionary where the key is the field name and the value is the embedding
        
        vectorStore.similarityMultiSearchWithScore = async (query: any, k: number, filter?: any, _callbacks = undefined) => {
            console.log("Entering similarityMultiSearchWithScore")
            const startTime = new Date()
            const obj = JSON.parse(query)
            const toEmbed = obj['to_embed']
            const directFilters = obj['direct_filters']
            const keys = Object.keys(toEmbed)
            
            //get the embeddings for all the keys in the query
            const embeddingsArray = await vectorStore.embeddings.embedDocuments(keys.map((key) => toEmbed[key]))
            const embeddingsByKey = {} as any
            keys.forEach((key, index) => {
                embeddingsByKey[key] = embeddingsArray[index]
            })

            console.log("total time taken to get embeddings", (new Date().getTime() - startTime.getTime()) / 1000)
            // replace the templates in the query with the embeddings ie:
            // [skills] will be replaced with the string representation of the embedding for skills
            const replaceTemplatesWithEmbeddings = (str: string) => {
                const words = str.match(/\[(.*?)\]/g)
                console.log("words: ", words)
                words?.forEach((word) => {
                    //find the embedding for this template
                    const wordEmbedding = embeddingsByKey[word.replace('[','').replace( ']','')]
                    console.log("wordEmbedding: ", wordEmbedding)
                    console.log("Type of wordEmbedding: ", typeof wordEmbedding)
                    // replace the template with the string representation of the embedding
                    var embeddingString = ''
                    if (typeof wordEmbedding === 'object') {
                        embeddingString = `'[${wordEmbedding.join(',')}]'`
                    }
                    else {
                        embeddingString = `'${directFilters[word.replace('[','').replace( ']','')]}'`
                    } 
                    str = str.replace(new RegExp( word.replace('[','\\[').replace( ']','\\]'), 'g'), embeddingString)
                })
                return str
            }

            let queryString = replaceTemplatesWithEmbeddings(full_query)
            console.log('demo queryString: ', queryString.replace(/\n+/g, ' ').replace(/\s+/g, ' ').trim())
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

            const documents = await conn.query(queryString)
            conn.release()
            console.log("result:", documents.rows.length, "documents")
            const results = [] as [TypeORMVectorStoreDocument, number][]
            for (const doc of documents.rows) {
                if (doc.pageContent != null) {
                    const document = new Document(doc) as TypeORMVectorStoreDocument
                    document.id = doc.id
                    results.push([document, 0.01])
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

module.exports = { nodeClass: Postgres_Multi_Existing_VectorStores }
