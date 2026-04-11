/**
 * Database config per UI environment (uat | stage | prod).
 *
 * Variables are read in order:
 *   1. `${SCOPE}_${NAME}` — e.g. UAT_POSTGRES_HOST (recommended when multiple envs in one .env)
 *   2. `${NAME}` — e.g. POSTGRES_HOST (single-env or shared defaults)
 *   3. Built-in default (local dev only; production should set env vars)
 *
 * Load `.env` from server.js before this module is required.
 */

function envScoped(scope, name, fallback) {
  const scoped = process.env[`${scope}_${name}`];
  if (scoped !== undefined && scoped !== "") {
    return scoped;
  }
  const plain = process.env[name];
  if (plain !== undefined && plain !== "") {
    return plain;
  }
  return fallback;
}

function parsePort(value, fallback) {
  const n = parseInt(String(value), 10);
  return Number.isFinite(n) ? n : fallback;
}

function buildPostgres(scope, defaults) {
  return {
    label: envScoped(scope, "POSTGRES_LABEL", defaults.label),
    host: envScoped(scope, "POSTGRES_HOST", defaults.host),
    port: parsePort(envScoped(scope, "POSTGRES_PORT", String(defaults.port)), defaults.port),
    user: envScoped(scope, "POSTGRES_USER", defaults.user),
    password: envScoped(scope, "POSTGRES_PASSWORD", defaults.password),
    database: envScoped(scope, "POSTGRES_DATABASE", defaults.database)
  };
}

function buildCassandra(scope, defaults) {
  const pointsRaw = envScoped(
    scope,
    "CASSANDRA_CONTACT_POINTS",
    Array.isArray(defaults.contactPoints)
      ? defaults.contactPoints.join(",")
      : defaults.contactPoints
  );
  const contactPoints = String(pointsRaw)
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);

  const cfg = {
    label: envScoped(scope, "CASSANDRA_LABEL", defaults.label),
    contactPoints: contactPoints.length ? contactPoints : defaults.contactPoints,
    localDataCenter: envScoped(scope, "CASSANDRA_LOCAL_DATA_CENTER", defaults.localDataCenter),
    keyspace: envScoped(scope, "CASSANDRA_KEYSPACE", defaults.keyspace)
  };

  const user = envScoped(scope, "CASSANDRA_USERNAME", defaults.username);
  const pass = envScoped(scope, "CASSANDRA_PASSWORD", defaults.password);
  if (user) {
    cfg.username = user;
  }
  if (pass) {
    cfg.password = pass;
  }

  return cfg;
}

function buildMongo(scope, defaults) {
  return {
    label: envScoped(scope, "MONGODB_LABEL", defaults.label),
    uri: envScoped(scope, "MONGODB_URI", defaults.uri),
    database: envScoped(scope, "MONGODB_DATABASE", defaults.database)
  };
}

const dbConfigsByEnvironment = {
  uat: {
    postgres: buildPostgres("UAT", {
      label: "PostgreSQL",
      host: "127.0.0.1",
      port: 5432,
      user: "postgres",
      password: "",
      database: "postgres"
    }),
    cassandra: buildCassandra("UAT", {
      label: "Cassandra",
      contactPoints: ["127.0.0.1"],
      localDataCenter: "datacenter1",
      keyspace: "system",
      username: "",
      password: ""
    }),
    mongodb: buildMongo("UAT", {
      label: "MongoDB",
      uri: "mongodb://127.0.0.1:27017",
      database: "test"
    })
  },
  stage: {
    postgres: buildPostgres("STAGE", {
      label: "PostgreSQL",
      host: "127.0.0.1",
      port: 5432,
      user: "stage_postgres",
      password: "stage_postgres123",
      database: "stage_sampledb"
    }),
    cassandra: buildCassandra("STAGE", {
      label: "Cassandra",
      contactPoints: ["127.0.0.1"],
      localDataCenter: "datacenter1",
      keyspace: "system",
      username: "",
      password: ""
    }),
    mongodb: buildMongo("STAGE", {
      label: "MongoDB",
      uri: "mongodb://127.0.0.1:27017",
      database: "stage_admin"
    })
  },
  prod: {
    postgres: buildPostgres("PROD", {
      label: "PostgreSQL",
      host: "127.0.0.1",
      port: 5432,
      user: "postgres",
      password: "",
      database: "postgres"
    }),
    cassandra: buildCassandra("PROD", {
      label: "Cassandra",
      contactPoints: ["127.0.0.1"],
      localDataCenter: "datacenter1",
      keyspace: "system",
      username: "",
      password: ""
    }),
    mongodb: buildMongo("PROD", {
      label: "MongoDB",
      uri: "mongodb://127.0.0.1:27017",
      database: "admin"
    })
  }
};

function getEnvironmentConfig(environment) {
  const config = dbConfigsByEnvironment[environment];

  if (!config) {
    throw new Error("Unsupported environment selection.");
  }

  return config;
}

module.exports = {
  dbConfigsByEnvironment,
  getEnvironmentConfig
};
