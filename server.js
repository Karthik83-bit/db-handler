const express = require("express");
const { Client: PgClient } = require("pg");
const { MongoClient } = require("mongodb");
const cassandra = require("cassandra-driver");
const {
  dbConfigsByEnvironment,
  getEnvironmentConfig
} = require("./db-config");

const app = express();
const port = process.env.PORT || 3000;
const DEFAULT_ROW_LIMIT = 100;
const DEFAULT_MONGO_SAMPLE_LIMIT = 25;
const IDENTIFIER_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;

app.use(express.json());
app.use(express.static("public"));

function assertValidIdentifier(value, label) {
  if (!IDENTIFIER_PATTERN.test(value)) {
    throw new Error(`Invalid ${label}.`);
  }
}

function normalizeFilters(filters) {
  if (!Array.isArray(filters)) {
    return [];
  }

  return filters
    .filter((filter) => filter && filter.field)
    .map((filter) => ({
      field: String(filter.field).trim(),
      value: filter.value
    }));
}

function normalizeEntities(entities) {
  if (!Array.isArray(entities)) {
    return [];
  }

  return entities
    .map((entity) => String(entity || "").trim())
    .filter(Boolean);
}

function getDatabaseConfig(environment, database) {
  const envConfig = getEnvironmentConfig(environment);
  const databaseConfig = envConfig[database];

  if (!databaseConfig) {
    throw new Error("Unsupported database selection.");
  }

  return databaseConfig;
}

async function withPostgresClient(config, callback) {
  const client = new PgClient(config);

  try {
    await client.connect();
    return await callback(client);
  } finally {
    await client.end().catch(() => {});
  }
}

async function withMongoClient(config, callback) {
  const client = new MongoClient(config.uri);

  try {
    await client.connect();
    return await callback(client.db(config.database));
  } finally {
    await client.close().catch(() => {});
  }
}

async function withCassandraClient(config, callback) {
  const authProvider =
    config.username && config.password
      ? new cassandra.auth.PlainTextAuthProvider(
          config.username,
          config.password
        )
      : undefined;

  const client = new cassandra.Client({
    ...config,
    authProvider
  });

  try {
    await client.connect();
    return await callback(client);
  } finally {
    await client.shutdown().catch(() => {});
  }
}

function flattenDocumentKeys(value, prefix = "", keys = new Set()) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return keys;
  }

  Object.entries(value).forEach(([key, nestedValue]) => {
    const currentKey = prefix ? `${prefix}.${key}` : key;
    keys.add(currentKey);

    if (
      nestedValue &&
      typeof nestedValue === "object" &&
      !Array.isArray(nestedValue) &&
      !(nestedValue instanceof Date)
    ) {
      flattenDocumentKeys(nestedValue, currentKey, keys);
    }
  });

  return keys;
}

function getMongoQueryValue(rawValue) {
  if (typeof rawValue !== "string") {
    return rawValue;
  }

  const trimmed = rawValue.trim();

  if (trimmed === "") {
    return "";
  }

  if (trimmed === "true") {
    return true;
  }

  if (trimmed === "false") {
    return false;
  }

  if (trimmed === "null") {
    return null;
  }

  if (!Number.isNaN(Number(trimmed))) {
    return Number(trimmed);
  }

  return trimmed;
}

function getSqlValue(rawValue) {
  if (typeof rawValue !== "string") {
    return rawValue;
  }

  const trimmed = rawValue.trim();

  if (trimmed === "") {
    return "";
  }

  if (trimmed === "true") {
    return true;
  }

  if (!Number.isNaN(Number(trimmed))) {
    return Number(trimmed);
  }

  return trimmed;
}

async function connectPostgres(config, environment) {
  return withPostgresClient(config, async (client) => {
    const result = await client.query("SELECT NOW() AS server_time");

    return {
      environment,
      database: config.label,
      success: true,
      message: "Connected successfully.",
      details: {
        host: config.host,
        database: config.database,
        serverTime: result.rows[0]?.server_time ?? null
      }
    };
  });
}

async function connectMongo(config, environment) {
  return withMongoClient(config, async (db) => {
    await db.command({ ping: 1 });

    return {
      environment,
      database: config.label,
      success: true,
      message: "Connected successfully.",
      details: {
        uri: config.uri,
        database: config.database,
        ping: "ok"
      }
    };
  });
}

async function connectCassandra(config, environment) {
  return withCassandraClient(config, async (client) => {
    const result = await client.execute(
      "SELECT release_version FROM system.local"
    );

    return {
      environment,
      database: config.label,
      success: true,
      message: "Connected successfully.",
      details: {
        contactPoints: config.contactPoints,
        keyspace: config.keyspace,
        version: result.rows[0]?.release_version ?? "unknown"
      }
    };
  });
}

async function connectToDatabase(database, environment) {
  const config = getDatabaseConfig(environment, database);

  switch (database) {
    case "postgres":
      return connectPostgres(config, environment);
    case "mongodb":
      return connectMongo(config, environment);
    case "cassandra":
      return connectCassandra(config, environment);
    default:
      throw new Error("Unsupported database selection.");
  }
}

async function listPostgresEntities(config) {
  return withPostgresClient(config, async (client) => {
    const result = await client.query(
      `SELECT table_name
       FROM information_schema.tables
       WHERE table_schema = 'public'
         AND table_type = 'BASE TABLE'
       ORDER BY table_name`
    );

    return result.rows.map((row) => ({
      value: row.table_name,
      label: row.table_name
    }));
  });
}

async function listMongoEntities(config) {
  return withMongoClient(config, async (db) => {
    const collections = await db.listCollections({}, { nameOnly: true }).toArray();

    return collections.map((collection) => ({
      value: collection.name,
      label: collection.name
    }));
  });
}

async function listCassandraEntities(config) {
  return withCassandraClient(config, async (client) => {
    const result = await client.execute(
      `SELECT table_name
       FROM system_schema.tables
       WHERE keyspace_name = ?`,
      [config.keyspace],
      { prepare: true }
    );

    return result.rows
      .map((row) => row.table_name)
      .sort()
      .map((tableName) => ({
        value: tableName,
        label: tableName
      }));
  });
}

async function listEntities(database, environment) {
  const config = getDatabaseConfig(environment, database);

  switch (database) {
    case "postgres":
      return listPostgresEntities(config);
    case "mongodb":
      return listMongoEntities(config);
    case "cassandra":
      return listCassandraEntities(config);
    default:
      throw new Error("Unsupported database selection.");
  }
}

async function describePostgresEntity(config, entity) {
  assertValidIdentifier(entity, "table name");

  return withPostgresClient(config, async (client) => {
    const result = await client.query(
      `SELECT column_name, data_type
       FROM information_schema.columns
       WHERE table_schema = 'public'
         AND table_name = $1
       ORDER BY ordinal_position`,
      [entity]
    );

    return result.rows.map((row) => ({
      name: row.column_name,
      type: row.data_type
    }));
  });
}

async function describeMongoEntity(config, entity) {
  return withMongoClient(config, async (db) => {
    const documents = await db
      .collection(entity)
      .find({})
      .limit(DEFAULT_MONGO_SAMPLE_LIMIT)
      .toArray();

    const keys = new Set();
    documents.forEach((document) => flattenDocumentKeys(document, "", keys));

    if (keys.size === 0) {
      keys.add("_id");
    }

    return Array.from(keys)
      .sort()
      .map((key) => ({
        name: key,
        type: "mixed"
      }));
  });
}

async function describeCassandraEntity(config, entity) {
  assertValidIdentifier(entity, "table name");

  return withCassandraClient(config, async (client) => {
    const result = await client.execute(
      `SELECT column_name, type
       FROM system_schema.columns
       WHERE keyspace_name = ?
         AND table_name = ?`,
      [config.keyspace, entity],
      { prepare: true }
    );

    return result.rows
      .map((row) => ({
        name: row.column_name,
        type: row.type
      }))
      .sort((left, right) => left.name.localeCompare(right.name));
  });
}

async function describeEntity(database, environment, entity) {
  const config = getDatabaseConfig(environment, database);

  switch (database) {
    case "postgres":
      return describePostgresEntity(config, entity);
    case "mongodb":
      return describeMongoEntity(config, entity);
    case "cassandra":
      return describeCassandraEntity(config, entity);
    default:
      throw new Error("Unsupported database selection.");
  }
}

async function queryPostgres(config, entity, filters) {
  assertValidIdentifier(entity, "table name");
  filters.forEach((filter) => assertValidIdentifier(filter.field, "field name"));

  return withPostgresClient(config, async (client) => {
    const whereClauses = filters.map(
      (filter, index) => `"${filter.field}" = $${index + 1}`
    );
    const sql =
      whereClauses.length > 0
        ? `SELECT * FROM "${entity}" WHERE ${whereClauses.join(" AND ")} LIMIT ${DEFAULT_ROW_LIMIT}`
        : `SELECT * FROM "${entity}" LIMIT ${DEFAULT_ROW_LIMIT}`;
    const values = filters.map((filter) => getSqlValue(filter.value));
    const result = await client.query(sql, values);

    return {
      columns: result.fields.map((field) => field.name),
      rows: result.rows
    };
  });
}

async function queryMongo(config, entity, filters) {
  return withMongoClient(config, async (db) => {
    const query = {};
    filters.forEach((filter) => {
      query[filter.field] = getMongoQueryValue(filter.value);
    });

    const rows = await db
      .collection(entity)
      .find(query)
      .limit(DEFAULT_ROW_LIMIT)
      .toArray();

    const columns = new Set();
    rows.forEach((row) => {
      Object.keys(row).forEach((key) => columns.add(key));
    });

    return {
      columns: Array.from(columns),
      rows
    };
  });
}

async function queryCassandra(config, entity, filters) {
  assertValidIdentifier(entity, "table name");
  filters.forEach((filter) => assertValidIdentifier(filter.field, "field name"));

  return withCassandraClient(config, async (client) => {
    const values = filters.map((filter) => getSqlValue(filter.value));
    const whereClause =
      filters.length > 0
        ? ` WHERE ${filters
            .map((filter) => `${filter.field} = ?`)
            .join(" AND ")} ALLOW FILTERING`
        : "";
    const query = `SELECT * FROM ${entity}${whereClause} LIMIT ${DEFAULT_ROW_LIMIT}`;
    const result = await client.execute(query, values, {
      prepare: filters.length > 0
    });
    const rows = result.rows.map((row) => ({ ...row }));
    const columns = new Set();

    rows.forEach((row) => {
      Object.keys(row).forEach((key) => columns.add(key));
    });

    return {
      columns: Array.from(columns),
      rows
    };
  });
}

async function runEntityQuery(database, environment, entity, filters) {
  const config = getDatabaseConfig(environment, database);
  const normalizedFilters = normalizeFilters(filters);

  switch (database) {
    case "postgres":
      return queryPostgres(config, entity, normalizedFilters);
    case "mongodb":
      return queryMongo(config, entity, normalizedFilters);
    case "cassandra":
      return queryCassandra(config, entity, normalizedFilters);
    default:
      throw new Error("Unsupported database selection.");
  }
}

function mergeRowsByEntity(rowsByEntity) {
  const merged = {};

  Object.entries(rowsByEntity).forEach(([entity, rows]) => {
    if (!rows.length) {
      return;
    }

    rows.forEach((row, index) => {
      Object.entries(row).forEach(([key, value]) => {
        const mergedKey = Object.prototype.hasOwnProperty.call(merged, key)
          ? `${entity}.${key}${index > 0 ? `.${index + 1}` : ""}`
          : key;
        merged[mergedKey] = value;
      });
    });
  });

  return merged;
}

async function mergeEntityData(database, environment, entities, joinField, joinValue) {
  assertValidIdentifier(joinField, "join field");

  const normalizedEntities = normalizeEntities(entities);

  if (normalizedEntities.length < 2) {
    throw new Error("Choose at least two tables or collections to merge.");
  }

  const rowsByEntityEntries = await Promise.all(
    normalizedEntities.map(async (entity) => {
      const result = await runEntityQuery(database, environment, entity, [
        {
          field: joinField,
          value: joinValue
        }
      ]);

      return [entity, result.rows];
    })
  );

  const rowsByEntity = Object.fromEntries(rowsByEntityEntries);

  return {
    joinField,
    joinValue,
    entities: normalizedEntities,
    rowsByEntity,
    mergedRecord: mergeRowsByEntity(rowsByEntity)
  };
}

app.get("/api/databases", (_req, res) => {
  const defaultEnvironment = "uat";
  const defaultConfig = dbConfigsByEnvironment[defaultEnvironment];

  res.json({
    environments: Object.keys(dbConfigsByEnvironment).map((value) => ({
      value,
      label: value.toUpperCase()
    })),
    options: Object.entries(defaultConfig).map(([value, config]) => ({
      value,
      label: config.label
    }))
  });
});

app.post("/api/connect", async (req, res) => {
  const { database, environment } = req.body;

  if (!database || !environment) {
    return res.status(400).json({
      success: false,
      message: "Please choose both environment and database."
    });
  }

  try {
    const result = await connectToDatabase(database, environment);
    return res.json(result);
  } catch (error) {
    return res.status(500).json({
      environment,
      database,
      success: false,
      message: "Connection failed.",
      error: error.message
    });
  }
});

app.get("/api/entities", async (req, res) => {
  const { environment, database } = req.query;

  if (!environment || !database) {
    return res.status(400).json({
      success: false,
      message: "Please provide both environment and database."
    });
  }

  try {
    const entities = await listEntities(database, environment);
    return res.json({
      success: true,
      environment,
      database,
      entities
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      message: "Failed to load entities.",
      error: error.message
    });
  }
});

app.get("/api/entity-fields", async (req, res) => {
  const { environment, database, entity } = req.query;

  if (!environment || !database || !entity) {
    return res.status(400).json({
      success: false,
      message: "Please provide environment, database, and entity."
    });
  }

  try {
    const fields = await describeEntity(database, environment, entity);
    return res.json({
      success: true,
      environment,
      database,
      entity,
      fields
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      message: "Failed to load fields.",
      error: error.message
    });
  }
});

app.post("/api/query", async (req, res) => {
  const { environment, database, entity, filters } = req.body;

  if (!environment || !database || !entity) {
    return res.status(400).json({
      success: false,
      message: "Please provide environment, database, and entity."
    });
  }

  try {
    const result = await runEntityQuery(database, environment, entity, filters);
    return res.json({
      success: true,
      environment,
      database,
      entity,
      filters: normalizeFilters(filters),
      rowCount: result.rows.length,
      columns: result.columns,
      rows: result.rows
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      message: "Query failed.",
      error: error.message
    });
  }
});

app.post("/api/merge", async (req, res) => {
  const { environment, database, entities, joinField, joinValue } = req.body;

  if (!environment || !database || !joinField || joinValue === undefined) {
    return res.status(400).json({
      success: false,
      message: "Please provide environment, database, entities, joinField, and joinValue."
    });
  }

  try {
    const result = await mergeEntityData(
      database,
      environment,
      entities,
      joinField,
      joinValue
    );

    return res.json({
      success: true,
      environment,
      database,
      ...result
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      message: "Merge failed.",
      error: error.message
    });
  }
});

app.listen(port, () => {
  console.log(`Server running at http://localhost:${port}`);
});
