import fs from 'fs';

const metadata = {
  "hubs": [
    {
      "table_name": "FLIGHT",
      "business_key": "FlightID",
      "source_table_AI": "AI_FLIGHT_DETAILS"
	  "source_table_SJ": "SJ_FLIGHT_DETAILS"
    },
    {
      "table_name": "AIRPORT",
      "business_key": "AirportCode",
      "source_table_AI": "AI_AIRPORT_DETAILS"
	  "source_table_SJ": "SJ_AIRPORT_DETAILS"
    }
  ]
};

function generateHub(table_name, business_key, source_table_AI, source_table_SJ) {
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
  '${source_table_AI}' AS REC_SRC
FROM \${ref("${source_table_AI}")}
WHERE ${business_key} IS NOT NULL
GROUP BY ${business_key}
UNION ALL
SELECT
  MD5(${business_key}) AS HK_${business_key},
  ${business_key},
  CURRENT_TIMESTAMP() AS LOAD_DTS,
  '${source_table_SJ}' AS REC_SRC
FROM \${ref("${source_table_SJ}")}
WHERE ${business_key} IS NOT NULL
GROUP BY ${business_key};
`;
}

const sqlxScripts = [];

metadata.hubs.forEach(hub => {
  sqlxScripts.push(generateHub(hub.table_name, hub.business_key, hub.source_table_AI, hub.source_table_SJ));
});

// Write to a .sqlx file
fs.writeFileSync("generated_hubs.sqlx", sqlxScripts.join("\n\n"));

console.log("✅ SQLX file 'generated_hubs.sqlx' has been created.");