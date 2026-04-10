
const dbConfigsByEnvironment = {
  uat: {
    postgres: {
      label: "PostgreSQL",
      host: "34.93.233.58",
      port: "5432",
      user: "kotak_uat_rw",
      password: "TYnb2#819",
      database: "kotak_uat"
    },
    cassandra: {
      label: "Cassandra",
      contactPoints: ["34.93.148.99"],
      localDataCenter: "datacenter1",
      keyspace: "kotak_uat",
      username: "kotak_uat_rw",
      password: "K0+@k&U@t5O!50$"
    },
    mongodb: {
      label: "MongoDB",
      uri: "mongodb://mongorwuser:rw%404456@35.200.190.68:27017/mongostage?directConnection=true&tls=false",
      database: "mongostage"
    }
  },
  stage: {
    postgres: {
      label: "PostgreSQL",
      host: "127.0.0.1",
      port: 5432,
      user: "stage_postgres",
      password: "stage_postgres123",
      database: "stage_sampledb"
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
      database: "stage_admin"
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
