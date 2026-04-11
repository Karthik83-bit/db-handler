
const dbConfigsByEnvironment = {
  uat: {
    postgres: {
      label:process.env.POSTGRES_LABEL,
      host: process.env.POSTGRES_HOST,
      port: process.env.POSTGRES_PORT,
      user: process.env.POSTGRES_USER ,
      password: process.env.POSTGRES_PASSWORD,
      database: process.env.POSTGRES_DATABASE ,
    },
    cassandra: {
      label: process.env.CASSANDRA_LABEL,
      contactPoints: process.env.CASSANDRA_CONTACT_POINTS ? process.env.CASSANDRA_CONTACT_POINTS.split(",") : ["34.93.148.99"],
      localDataCenter: process.env.CASSANDRA_LOCAL_DATA_CENTER,
      keyspace: process.env.CASSANDRA_KEYSPACE,
      username: process.env.CASSANDRA_USERNAME,
      password: process.env.CASSANDRA_PASSWORD,
    },
    mongodb: {
      label: process.env.MONGODB_LABEL,
      uri: process.env.MONGODB_URI,
      database: process.env.MONGODB_DATABASE
    }
  },
  stage: {
    postgres: {
      label: process.env.POSTGRES_LABEL || "PostgreSQL",
      host: process.env.POSTGRES_HOST || "127.0.0.1",
      port: process.env.POSTGRES_PORT || 5432,
      user: process.env.POSTGRES_USER || "stage_postgres",
      password: process.env.POSTGRES_PASSWORD || "stage_postgres123",
      database: process.env.POSTGRES_DATABASE || "stage_sampledb"
    },
    cassandra: {
      label: process.env.CASSANDRA_LABEL || "Cassandra",
      contactPoints: process.env.CASSANDRA_CONTACT_POINTS ? process.env.CASSANDRA_CONTACT_POINTS.split(",") : ["127.0.0.1"],
      localDataCenter: process.env.CASSANDRA_LOCAL_DATA_CENTER || "datacenter1",
      keyspace: process.env.CASSANDRA_KEYSPACE ||  "system"
    },
    mongodb: {
      label: process.env.MONGODB_LABEL || "MongoDB",
      uri: process.env.MONGODB_URI || "mongodb://127.0.0.1:27017",
      database: process.env.MONGODB_DATABASE || "stage_admin"
    }
  },
  prod: {
    postgres: {
      label: "PostgreSQL",
      host: "127.0.0.1",
      port: 5432,
      user: "prod_postgres",
      password: "prod_postgres123",
      database: "prod_sampledb"
    },
    cassandra: {
      label: "Cassandra",
      contactPoints: ["127.0.0.1"],
      localDataCenter: "datacenter1",
      keyspace: "system"
    },
    mongodb: {
      label: "MongoDB",
      uri: "mongodb://127.0.0.1:27017",
      database: "prod_admin"
    }
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
