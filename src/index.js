const { MongoClient } = require('mongodb')
class Indempotent {
	constructor({ db = {}, tableName }) {
		// * Inputs
		this.db = {
			name: db.name || process.env.INDEMPOTENT_DB_NAME,
			options: db.options || {},
			uri: db.uri || process.env.INDEMPOTENT_DB_URI,
		}
		this.tableName = tableName || process.env.INDEMPOTENT_TABLE_NAME

		// * Database connections
		this.client = undefined

		// * Run immediately
		this._setupIndexes()
	}

	async _collection() {
		if (typeof this.client === 'undefined') {
			this.client = new MongoClient(this.db.uri, {
				useNewUrlParser: true,
				useUnifiedTopology: true,
				...this.db.options,
			})
			await this.client.connect()
		}
		const db = this.client.db(this.db.name)
		const collection = db.collection(this.tableName)
		return collection
	}

	async _setupIndexes() {
		const collection = await this._collection()
		await Promise.allSettled([
			collection.createIndex({ createdAt: 1 }, { expireAfterSeconds: 3600 }),
			collection.createIndex({ id: 1, source: 1, type: 1 }, { unique: true }),
		])

	}

	async lock({ cloudevent }) {
		const collection = await this._collection()
		const { id, source, type } = cloudevent

		let stop = false
		try {
			await collection.insertOne({
				createdAt: new Date(),
				id,
				source,
				type,
			})
		} catch (_err) {
			stop = true
		}
		return stop
	}

	async unlock({ cloudevent }) {
		const collection = await this._collection()
		const { id, source, type } = cloudevent

		await collection.deleteMany({ id, source, type })
	}
}

module.exports = { Indempotent }
