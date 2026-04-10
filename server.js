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
const FIELD_PATH_PATTERN = /^[A-Za-z_][A-Za-z0-9_.]*$/;

app.use(express.json());
app.use(express.static("public"));

function assertValidIdentifier(value, label) {
  if (!IDENTIFIER_PATTERN.test(value)) {
    throw new Error(`Invalid ${label}.`);
  }
}

function assertValidFieldPath(value, label) {
  if (!FIELD_PATH_PATTERN.test(value)) {
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

function getSourceKey(database, entity, sourceId = "") {
  return `${database}:${entity}:${sourceId}`;
}

function parseSourceKey(sourceKey) {
  const [database = "", entity = "", sourceId = ""] = String(sourceKey || "").split(":");
  return {
    database,
    entity,
    sourceId
  };
}

function normalizeSelectedSources(sources) {
  if (!Array.isArray(sources)) {
    return [];
  }

  return sources
    .map((source, index) => {
      const database = String(source?.database || "").trim();
      const entity = String(source?.entity || "").trim();
      const sourceId = String(source?.sourceId || `source-${index + 1}`).trim();

      if (!database || !entity) {
        return null;
      }

      return {
        sourceId,
        database,
        entity,
        selectedFields: Array.isArray(source?.selectedFields)
          ? source.selectedFields
              .map((field) => String(field || "").trim())
              .filter(Boolean)
          : []
      };
    })
    .filter(Boolean);
}

function normalizeMergeMappings(mappings) {
  if (!Array.isArray(mappings)) {
    return [];
  }

  return mappings
    .map((mapping) => ({
      leftSourceId: String(mapping?.leftSourceId || "").trim(),
      leftDatabase: String(mapping?.leftDatabase || "").trim(),
      leftEntity: String(mapping?.leftEntity || "").trim(),
      leftField: String(mapping?.leftField || "").trim(),
      rightSourceId: String(mapping?.rightSourceId || "").trim(),
      rightDatabase: String(mapping?.rightDatabase || "").trim(),
      rightEntity: String(mapping?.rightEntity || "").trim(),
      rightField: String(mapping?.rightField || "").trim(),
      castType: String(mapping?.castType || "auto").trim().toLowerCase()
    }))
    .filter(
      (mapping) =>
        mapping.leftDatabase &&
        mapping.leftEntity &&
        mapping.leftField &&
        mapping.rightDatabase &&
        mapping.rightEntity &&
        mapping.rightField
    );
}

function normalizeSourceFilters(filters) {
  if (!Array.isArray(filters)) {
    return [];
  }

  return filters
    .map((filter) => ({
      sourceId: String(filter?.sourceId || "").trim(),
      database: String(filter?.database || "").trim(),
      entity: String(filter?.entity || "").trim(),
      field: String(filter?.field || "").trim(),
      value: filter?.value
    }))
    .filter(
      (filter) =>
        filter.database &&
        filter.entity &&
        filter.field &&
        filter.value !== undefined &&
        filter.value !== null &&
        String(filter.value).trim() !== ""
    );
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

function getNestedValue(record, path) {
  if (!path.includes(".")) {
    return record?.[path];
  }

  return path.split(".").reduce((current, segment) => {
    if (current === null || current === undefined) {
      return undefined;
    }

    return current[segment];
  }, record);
}

function castValue(value, castType = "auto") {
  if (value === null || value === undefined) {
    return null;
  }

  const normalizedType = String(castType || "auto").toLowerCase();

  if (normalizedType === "auto") {
    if (typeof value === "number" || typeof value === "boolean") {
      return value;
    }

    if (value instanceof Date) {
      return value.toISOString();
    }

    if (typeof value === "object") {
      if (typeof value.toHexString === "function") {
        return value.toHexString();
      }

      if (typeof value.toString === "function") {
        const stringValue = value.toString();
        if (stringValue !== "[object Object]") {
          return stringValue;
        }
      }

      return JSON.stringify(value);
    }

    const raw = String(value).trim();

    if (raw === "") {
      return "";
    }

    if (raw === "true") {
      return true;
    }

    if (raw === "false") {
      return false;
    }

    if (!Number.isNaN(Number(raw)) && raw !== "") {
      return Number(raw);
    }

    const dateValue = new Date(raw);
    if (!Number.isNaN(dateValue.getTime()) && /[-/:T]/.test(raw)) {
      return dateValue.toISOString();
    }

    return raw;
  }

  switch (normalizedType) {
    case "string":
      return String(value);
    case "number": {
      const parsed = Number(value);
      return Number.isNaN(parsed) ? null : parsed;
    }
    case "boolean": {
      if (typeof value === "boolean") {
        return value;
      }

      const lowered = String(value).trim().toLowerCase();
      if (["true", "1", "yes"].includes(lowered)) {
        return true;
      }
      if (["false", "0", "no"].includes(lowered)) {
        return false;
      }
      return null;
    }
    case "date": {
      const parsed = new Date(value);
      return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString();
    }
    default:
      return String(value);
  }
}

function valuesMatch(leftValue, rightValue, castType) {
  const leftCasted = castValue(leftValue, castType);
  const rightCasted = castValue(rightValue, castType);

  if (leftCasted === null || rightCasted === null) {
    return false;
  }

  return leftCasted === rightCasted;
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
    // CQL requires LIMIT before ALLOW FILTERING (not the reverse).
    const whereClause =
      filters.length > 0
        ? ` WHERE ${filters
            .map((filter) => `${filter.field} = ?`)
            .join(" AND ")} LIMIT ${DEFAULT_ROW_LIMIT} ALLOW FILTERING`
        : "";
    const query =
      filters.length > 0
        ? `SELECT * FROM ${entity}${whereClause}`
        : `SELECT * FROM ${entity} LIMIT ${DEFAULT_ROW_LIMIT}`;
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

function resolveSourceKey(selectedSources, database, entity, sourceId) {
  const candidates = selectedSources.filter(
    (source) => source.database === database && source.entity === entity
  );

  if (!candidates.length) {
    throw new Error("Each mapping must point to a selected table or collection.");
  }

  if (sourceId) {
    const exact = candidates.find((source) => source.sourceId === sourceId);
    if (!exact) {
      throw new Error("Each mapping must point to a selected table or collection.");
    }
    return getSourceKey(exact.database, exact.entity, exact.sourceId);
  }

  if (candidates.length > 1) {
    throw new Error(
      "Mapping is ambiguous. Select a specific source when using the same table or collection more than once."
    );
  }

  const [candidate] = candidates;
  return getSourceKey(candidate.database, candidate.entity, candidate.sourceId);
}

/**
 * Lookup (table + field + value) is step 0. Each mapping row must extend the FK
 * chain in order from that lookup table: lookup → next table → next table …
 * (e.g. A.id → B.id, then B.key → C.key). Returns directed edges for fetch + join.
 */
function buildLookupJoinChain(lookupSourceKey, orderedMappings, selectedSources) {
  const edges = [];
  let currentKey = lookupSourceKey;
  const visited = new Set([currentKey]);

  orderedMappings.forEach((m, i) => {
    const leftKey = resolveSourceKey(
      selectedSources,
      m.leftDatabase,
      m.leftEntity,
      m.leftSourceId
    );
    const rightKey = resolveSourceKey(
      selectedSources,
      m.rightDatabase,
      m.rightEntity,
      m.rightSourceId
    );

    if (leftKey === currentKey && rightKey !== currentKey && !visited.has(rightKey)) {
      edges.push({
        from: currentKey,
        to: rightKey,
        fromField: m.leftField,
        toField: m.rightField,
        castType: m.castType
      });
      currentKey = rightKey;
      visited.add(rightKey);
    } else if (rightKey === currentKey && leftKey !== currentKey && !visited.has(leftKey)) {
      edges.push({
        from: currentKey,
        to: leftKey,
        fromField: m.rightField,
        toField: m.leftField,
        castType: m.castType
      });
      currentKey = leftKey;
      visited.add(leftKey);
    } else {
      throw new Error(
        `Mapping ${i + 1} must extend the join chain from the current table. ` +
          `Current: "${currentKey}". This mapping has left="${leftKey}" right="${rightKey}". ` +
          `Order mappings as: lookup table → next table → next table (FK chain).`
      );
    }
  });

  if (visited.size !== selectedSources.length) {
    throw new Error(
      "Each selected table must appear exactly once in the join chain. " +
        `Connected ${visited.size} of ${selectedSources.length} sources. ` +
        `Use ${selectedSources.length - 1} mapping row(s) in order (one FK hop each).`
    );
  }

  return edges;
}

/**
 * Inner join along the chain: each edge adds matching rows from the next table.
 */
function mergeChainJoinEdges(chainEdges, rowsBySource, selectedSources) {
  const baseKey = chainEdges[0].from;
  let currentGroups = (rowsBySource[baseKey]?.rows || []).map((row) => ({
    [baseKey]: row
  }));

  for (const edge of chainEdges) {
    const nextGroups = [];
    currentGroups.forEach((group) => {
      const fromRow = group[edge.from];
      const toRows = rowsBySource[edge.to]?.rows || [];
      const matches = toRows.filter((rRow) =>
        valuesMatch(
          getNestedValue(fromRow, edge.fromField),
          getNestedValue(rRow, edge.toField),
          edge.castType
        )
      );
      matches.forEach((m) => {
        nextGroups.push({ ...group, [edge.to]: m });
      });
    });
    currentGroups = nextGroups;
  }

  return currentGroups.map((group) => flattenMergedGroup(group, selectedSources));
}

function flattenMergedGroup(group, selectedSources) {
  const flattened = {};

  selectedSources.forEach((source) => {
    const sourceKey = getSourceKey(source.database, source.entity, source.sourceId);
    const row = group[sourceKey];
    const selectedFields =
      Array.isArray(source.selectedFields) && source.selectedFields.length
        ? source.selectedFields
        : null;

    if (!row) {
      return;
    }

    Object.entries(row).forEach(([field, value]) => {
      if (selectedFields && !selectedFields.includes(field)) {
        return;
      }

      flattened[`${source.database}.${source.entity}.${field}`] = value;
    });
  });

  return flattened;
}

function buildMergeDebugSummary(selectedSources, rowsBySource, mappings, baseSourceKey) {
  const baseSource = rowsBySource[baseSourceKey];
  const baseRows = baseSource?.rows || [];
  const mappingStats = mappings.map((mapping) => {
    const leftSourceKey = getSourceKey(
      mapping.leftDatabase,
      mapping.leftEntity,
      mapping.leftSourceId
    );
    const rightSourceKey = getSourceKey(
      mapping.rightDatabase,
      mapping.rightEntity,
      mapping.rightSourceId
    );
    const leftRows = rowsBySource[leftSourceKey]?.rows || [];
    const rightRows = rowsBySource[rightSourceKey]?.rows || [];
    let pairMatches = 0;

    leftRows.forEach((leftRow) => {
      rightRows.forEach((rightRow) => {
        if (
          valuesMatch(
            getNestedValue(leftRow, mapping.leftField),
            getNestedValue(rightRow, mapping.rightField),
            mapping.castType
          )
        ) {
          pairMatches += 1;
        }
      });
    });

    return {
      leftSourceKey,
      leftField: mapping.leftField,
      rightSourceKey,
      rightField: mapping.rightField,
      castType: mapping.castType,
      leftRows: leftRows.length,
      rightRows: rightRows.length,
      pairMatches
    };
  });

  return {
    baseSourceKey,
    baseRows: baseRows.length,
    selectedSourceRows: Object.fromEntries(
      Object.entries(rowsBySource).map(([sourceKey, value]) => [sourceKey, value.rows.length])
    ),
    mappingStats
  };
}

async function mergeSelectedSources(environment, sources, mappings, sourceFilters) {
  const pushTrace = (message, details = null) =>
    console.log(
      `[cross-merge] ${new Date().toISOString()} ${message}`,
      details === null || details === undefined ? "" : details
    );

  async function querySourceByMappedValues(
    targetSource,
    targetField,
    rawValues
  ) {
    assertValidFieldPath(targetField, "mapping field");
    const values = Array.from(
      new Set(
        rawValues
          .filter((value) => value !== undefined && value !== null && String(value).trim() !== "")
          .map((value) => value)
      )
    );

    if (!values.length) {
      return {
        columns: [],
        rows: []
      };
    }

    const rows = [];
    const columns = new Set();

    for (const value of values) {
      const result = await runEntityQuery(
        targetSource.database,
        environment,
        targetSource.entity,
        [{ field: targetField, value }]
      );
      result.rows.forEach((row) => rows.push(row));
      result.columns.forEach((column) => columns.add(column));
    }

    const dedupedRows = Array.from(
      new Map(rows.map((row) => [JSON.stringify(row), row])).values()
    );

    return {
      columns: Array.from(columns),
      rows: dedupedRows
    };
  }

  const selectedSources = normalizeSelectedSources(sources);
  const normalizedMappings = normalizeMergeMappings(mappings);
  const normalizedSourceFilters = normalizeSourceFilters(sourceFilters);
  pushTrace("Received merge request.", {
    environment,
    sourceCount: selectedSources.length,
    mappingCount: normalizedMappings.length,
    filterCount: normalizedSourceFilters.length
  });

  if (selectedSources.length < 2) {
    throw new Error("Choose at least two tables or collections to merge.");
  }

  if (!normalizedMappings.length) {
    throw new Error("Add at least one field mapping before merging.");
  }

  if (!normalizedSourceFilters.length) {
    throw new Error("Provide a lookup value to start the merge chain.");
  }

  const [lookupFilter] = normalizedSourceFilters;
  const lookupSource = selectedSources.find(
    (source) =>
      source.database === lookupFilter.database &&
      source.entity === lookupFilter.entity &&
      (!lookupFilter.sourceId || lookupFilter.sourceId === source.sourceId)
  );

  if (!lookupSource) {
    throw new Error("Lookup source must match one selected table or collection.");
  }

  const orderedSelectedSources = [
    lookupSource,
    ...selectedSources.filter((source) => source.sourceId !== lookupSource.sourceId)
  ];
  pushTrace("Lookup source selected as base source.", {
    baseSource: getSourceKey(lookupSource.database, lookupSource.entity, lookupSource.sourceId),
    lookupFilter
  });
  pushTrace(
    "Resolved merge source order.",
    orderedSelectedSources.map((source) =>
      getSourceKey(source.database, source.entity, source.sourceId)
    )
  );

  const rowsBySource = Object.fromEntries(
    orderedSelectedSources.map((source) => [
      getSourceKey(source.database, source.entity, source.sourceId),
      {
        sourceId: source.sourceId,
        database: source.database,
        entity: source.entity,
        columns: [],
        rows: []
      }
    ])
  );
  const baseSourceKey = getSourceKey(
    orderedSelectedSources[0].database,
    orderedSelectedSources[0].entity,
    orderedSelectedSources[0].sourceId
  );

  const chainEdges = buildLookupJoinChain(
    baseSourceKey,
    normalizedMappings,
    orderedSelectedSources
  );
  pushTrace("Resolved FK join chain (lookup → each hop).", chainEdges);

  assertValidFieldPath(lookupFilter.field, "filter field");
  pushTrace("Querying lookup source with lookup filter.", {
    source: baseSourceKey,
    filter: {
      field: lookupFilter.field,
      value: lookupFilter.value
    }
  });
  const baseResult = await runEntityQuery(
    orderedSelectedSources[0].database,
    environment,
    orderedSelectedSources[0].entity,
    [
      {
        field: lookupFilter.field,
        value: lookupFilter.value
      }
    ]
  );
  rowsBySource[baseSourceKey] = {
    ...rowsBySource[baseSourceKey],
    columns: baseResult.columns,
    rows: baseResult.rows
  };
  pushTrace("Lookup source query completed.", {
    source: baseSourceKey,
    rowsFetched: baseResult.rows.length,
    columnsFetched: baseResult.columns.length
  });

  if ((rowsBySource[baseSourceKey]?.rows || []).length === 0) {
    pushTrace("Base source has no rows after lookup filter. Returning empty merge.");
    const debug = buildMergeDebugSummary(
      orderedSelectedSources,
      rowsBySource,
      normalizedMappings,
      baseSourceKey
    );
    return {
      sources: orderedSelectedSources,
      mappings: normalizedMappings,
      sourceFilters: normalizedSourceFilters,
      mergedRows: [],
      mergedColumns: [],
      rowsBySource,
      debug
    };
  }

  for (const edge of chainEdges) {
    const fromRows = rowsBySource[edge.from]?.rows || [];
    const targetSource = orderedSelectedSources.find(
      (source) =>
        getSourceKey(source.database, source.entity, source.sourceId) === edge.to
    );
    const sourceValues = fromRows.map((row) => getNestedValue(row, edge.fromField));
    pushTrace("Chain fetch (FK hop).", {
      from: `${edge.from}.${edge.fromField}`,
      to: `${edge.to}.${edge.toField}`,
      castType: edge.castType,
      inputRowCount: sourceValues.length,
      fromLookupSource: edge.from === baseSourceKey
    });
    const result = await querySourceByMappedValues(
      targetSource,
      edge.toField,
      sourceValues
    );
    rowsBySource[edge.to] = {
      ...rowsBySource[edge.to],
      columns: result.columns,
      rows: result.rows
    };
    pushTrace("Chain fetch completed.", {
      source: edge.to,
      rowsFetched: result.rows.length,
      columnsFetched: result.columns.length
    });
  }

  const mergedRows = mergeChainJoinEdges(
    chainEdges,
    rowsBySource,
    orderedSelectedSources
  );
  const debug = buildMergeDebugSummary(
    orderedSelectedSources,
    rowsBySource,
    normalizedMappings,
    baseSourceKey
  );
  pushTrace("Merge completed.", {
    mergedRows: mergedRows.length,
    mergedColumns: Array.from(new Set(mergedRows.flatMap((row) => Object.keys(row)))).length
  });

  return {
    sources: orderedSelectedSources,
    mappings: normalizedMappings,
    sourceFilters: normalizedSourceFilters,
    mergedRows,
    mergedColumns: Array.from(
      new Set(mergedRows.flatMap((row) => Object.keys(row)))
    ),
    rowsBySource,
    debug
  };
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

app.post("/api/cross-merge", async (req, res) => {
  const { environment, sources, mappings, sourceFilters } = req.body;

  if (!environment) {
    return res.status(400).json({
      success: false,
      message: "Please provide an environment."
    });
  }

  try {
    const result = await mergeSelectedSources(
      environment,
      sources,
      mappings,
      sourceFilters
    );

    return res.json({
      success: true,
      environment,
      ...result,
      mergedCount: result.mergedRows.length
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      message: "Cross-database merge failed.",
      error: error.message
    });
  }
});

app.listen(port, () => {
  console.log(`Server running at http://localhost:${port}`);
});
