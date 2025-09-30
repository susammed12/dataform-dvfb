const metadata = {
  "hubs": [
    {
      "table_name": "FLIGHT",
      "business_key": "FlightID",
      "source_table": "AI_FLIGHT_DETAILS"
    },
    {
      "table_name": "AIRPORT",
      "business_key": "AirportCode",
      "source_table": "AI_AIRPORT_DETAILS"
    }
  ]
};

function generateHub(table_name, business_key, source_table) {
  return `
config {
  type: "table",
  schema: "raw_vault",
  tags: ["hub"]
}

SELECT
  MD5(${business_key}) AS HK_${business_key},
  ${business_key},
  CURRENT_TIMESTAMP() AS LOAD_DTS,
  '${source_table}' AS REC_SRC
FROM ${source_table}
WHERE ${business_key} IS NOT NULL
GROUP BY ${business_key};
`;
}

const sqlxScripts = [];

metadata.hubs.forEach(hub => {
  sqlxScripts.push(generateHub(hub.table_name, hub.business_key, hub.source_table));
});

sqlxScripts.forEach(script => console.log(script));